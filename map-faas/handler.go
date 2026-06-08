package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v2"
)

type Request struct {
	RequestID    string                 `json:"request_id"`
	ParsedQuery  map[string]interface{} `json:"parsed_query"`
	UserLocation *UserLocation          `json:"user_location,omitempty"`
}

type UserLocation struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Response struct {
	RequestID string      `json:"request_id"`
	Result    interface{} `json:"result"`
	Error     string      `json:"error,omitempty"`
}

var (
	neo4jDriver neo4j.DriverWithContext
	osClient    *opensearch.Client
	snsClient   *sns.Client
	snsTopicARN string
	nodesIndex  string
)

func init() {
	dbURI := os.Getenv("NEO4J_URI")
	dbUser := os.Getenv("NEO4J_USER")
	dbPassword := os.Getenv("NEO4J_PASSWORD")

	var err error
	neo4jDriver, err = neo4j.NewDriverWithContext(dbURI, neo4j.BasicAuth(dbUser, dbPassword, ""))
	if err != nil {
		log.Fatalf("Failed to create Neo4j driver: %v", err)
	}

	osURL := os.Getenv("OPENSEARCH_URL")
	if osURL == "" {
		log.Println("OPENSEARCH_URL not set, OpenSearch features disabled")
	} else {
		osClient, err = opensearch.NewClient(opensearch.Config{
			Addresses: []string{osURL},
			Username:  os.Getenv("OPENSEARCH_USER"),
			Password:  os.Getenv("OPENSEARCH_PASSWORD"),
		})
		if err != nil {
			log.Printf("Failed to create OpenSearch client: %v", err)
		} else {
			log.Println("OpenSearch client initialized")
		}
	}

	nodesIndex = os.Getenv("OPENSEARCH_NODES_INDEX")
	if nodesIndex == "" {
		nodesIndex = "nodes"
		log.Printf("OPENSEARCH_NODES_INDEX not set, using default: %s", nodesIndex)
	}

	snsTopicARN = os.Getenv("SNS_TOPIC_ARN")
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		cfg.BaseEndpoint = &endpoint
	}

	snsClient = sns.NewFromConfig(cfg)
}

func Handle(ctx context.Context, req []byte) (string, error) {
	var request Request
	if err := json.Unmarshal(req, &request); err != nil {
		log.Printf("Failed to unmarshal request: %v", err)
		return "", err
	}

	log.Printf("Processing request: %s", request.RequestID)

	queryType, ok := request.ParsedQuery["query_type"].(string)
	if !ok {
		resp := Response{
			RequestID: request.RequestID,
			Error:     "missing query_type",
		}
		body, _ := json.Marshal(resp)
		return string(body), nil
	}

	params, ok := request.ParsedQuery["params"].(map[string]interface{})
	if !ok {
		resp := Response{
			RequestID: request.RequestID,
			Error:     "missing params",
		}
		body, _ := json.Marshal(resp)
		return string(body), nil
	}

	var result interface{}
	var err error

	switch queryType {
	case "proximity", "nearest":
		if osClient == nil {
			err = fmt.Errorf("OpenSearch not available for %s queries", queryType)
		} else {
			result, err = searchInOpenSearch(ctx, queryType, params, request.UserLocation)
		}
	case "shortest_path", "route_details", "filter_by":
		result, err = executeNeo4jQuery(ctx, queryType, params)
	default:
		err = fmt.Errorf("unsupported query_type: %s", queryType)
	}

	if err != nil {
		resp := Response{
			RequestID: request.RequestID,
			Error:     err.Error(),
		}
		body, _ := json.Marshal(resp)
		return string(body), nil
	}

	publishToSNS(ctx, request.RequestID, result)

	resp := Response{
		RequestID: request.RequestID,
		Result:    result,
	}
	body, _ := json.Marshal(resp)
	return string(body), nil
}

func searchInOpenSearch(ctx context.Context, queryType string, params map[string]interface{}, userLocation *UserLocation) (interface{}, error) {
	switch queryType {
	case "proximity":
		return proximitySearch(ctx, params, userLocation)
	case "nearest":
		return nearestSearch(ctx, params, userLocation)
	default:
		return nil, fmt.Errorf("OS does not support %s", queryType)
	}
}

func resolveLocationRef(ctx context.Context, locationRef string, userLocation *UserLocation) (lat, lon float64, err error) {
	if osClient != nil && locationRef != "" {
		query := map[string]interface{}{
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					"name": map[string]interface{}{
						"query":     locationRef,
						"fuzziness": "AUTO",
					},
				},
			},
			"size": 1,
		}

		body, err := json.Marshal(query)
		if err == nil {
			res, err := osClient.Search(
				osClient.Search.WithIndex(nodesIndex),
				osClient.Search.WithBody(bytes.NewReader(body)),
				osClient.Search.WithContext(ctx),
			)
			if err == nil {
				defer func(Body io.ReadCloser) {
					err := Body.Close()
					if err != nil {
						log.Printf("Error Occurred, %v", err)
					}
				}(res.Body)
				var result map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&result); err == nil {
					hits, ok := result["hits"].(map[string]interface{})
					if ok {
						hitHits, ok := hits["hits"].([]interface{})
						if ok && len(hitHits) > 0 {
							source := hitHits[0].(map[string]interface{})["_source"].(map[string]interface{})
							lat = source["lat"].(float64)
							lon = source["lon"].(float64)
							log.Printf("Found location '%s' -> (%f, %f)", locationRef, lat, lon)
							return lat, lon, nil
						}
					}
				}
			}
		}
	}

	if userLocation != nil {
		log.Printf("Location '%s' not found, falling back to user location (%f, %f)", locationRef, userLocation.Lat, userLocation.Lon)
		return userLocation.Lat, userLocation.Lon, nil
	}

	return 0, 0, fmt.Errorf("location '%s' not found and no user location provided", locationRef)
}

func proximitySearch(ctx context.Context, params map[string]interface{}, userLocation *UserLocation) (interface{}, error) {
	var lat, lon float64

	if latVal, ok := params["lat"].(float64); ok {
		if lonVal, ok := params["lon"].(float64); ok {
			lat, lon = latVal, lonVal
			log.Printf("Using direct coordinates: (%f, %f)", lat, lon)
		}
	}

	if locationRef, ok := params["location_ref"].(string); ok && lat == 0 {
		var err error
		lat, lon, err = resolveLocationRef(ctx, locationRef, userLocation)
		if err != nil {
			return nil, err
		}
	}

	if lat == 0 && lon == 0 {
		return nil, fmt.Errorf("no location provided")
	}

	radius, ok := params["radius_km"].(float64)
	if !ok {
		radius = 1.0
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"geo_distance": map[string]interface{}{
				"distance": fmt.Sprintf("%.1fkm", radius),
				"location": map[string]float64{"lat": lat, "lon": lon},
			},
		},
		"size": 20,
	}

	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	res, err := osClient.Search(
		osClient.Search.WithIndex(nodesIndex),
		osClient.Search.WithBody(bytes.NewReader(body)),
		osClient.Search.WithContext(ctx),
	)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error Occurred, %v", err)
		}
	}(res.Body)

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func nearestSearch(ctx context.Context, params map[string]interface{}, userLocation *UserLocation) (interface{}, error) {
	poiType, ok := params["poi_type"].(string)
	if !ok {
		return nil, fmt.Errorf("missing poi_type in params")
	}

	var lat, lon float64

	if locationRef, ok := params["location_ref"].(string); ok {
		var err error
		lat, lon, err = resolveLocationRef(ctx, locationRef, userLocation)
		if err != nil {
			return nil, err
		}
	} else if latVal, ok := params["lat"].(float64); ok {
		if lonVal, ok := params["lon"].(float64); ok {
			lat, lon = latVal, lonVal
			log.Printf("Using direct coordinates: (%f, %f)", lat, lon)
		}
	} else if userLocation != nil {
		lat, lon = userLocation.Lat, userLocation.Lon
		log.Printf("No location_ref, using user location: (%f, %f)", lat, lon)
	}

	if lat == 0 && lon == 0 {
		return nil, fmt.Errorf("no location provided")
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"type": poiType,
						},
					},
				},
				"filter": []map[string]interface{}{
					{
						"geo_distance": map[string]interface{}{
							"distance": "10km",
							"location": map[string]float64{"lat": lat, "lon": lon},
						},
					},
				},
			},
		},
		"sort": []map[string]interface{}{
			{
				"_geo_distance": map[string]interface{}{
					"location": map[string]float64{"lat": lat, "lon": lon},
					"order":    "asc",
					"unit":     "km",
				},
			},
		},
		"size": 5,
	}

	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	res, err := osClient.Search(
		osClient.Search.WithIndex(nodesIndex),
		osClient.Search.WithBody(bytes.NewReader(body)),
		osClient.Search.WithContext(ctx),
	)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error Occurred, %v", err)
		}
	}(res.Body)

	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func executeNeo4jQuery(ctx context.Context, queryType string, params map[string]interface{}) (interface{}, error) {
	session := neo4jDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer func(session neo4j.SessionWithContext, ctx context.Context) {
		err := session.Close(ctx)
		if err != nil {
			log.Printf("Error Occurred, %v", err)
		}
	}(session, ctx)

	var cypher string

	switch queryType {
	case "shortest_path":
		fromRef, ok := params["from_ref"].(string)
		if !ok {
			return nil, fmt.Errorf("missing from_ref in params")
		}
		toRef, ok := params["to_ref"].(string)
		if !ok {
			return nil, fmt.Errorf("missing to_ref in params")
		}

		cypher = `
			MATCH (start:Node {name: $from_ref})
			MATCH (end:Node {name: $to_ref})
			MATCH path = shortestPath((start)-[:ROAD*]-(end))
			RETURN 
				[n in nodes(path) | n.name] as path,
				reduce(total = 0, r in relationships(path) | total + r.distance) as total_distance
		`
		params["from_ref"] = fromRef
		params["to_ref"] = toRef

	case "route_details":
		routeRef, ok := params["route_ref"].(string)
		if !ok {
			return nil, fmt.Errorf("missing route_ref in params")
		}
		cypher = `
			MATCH (r:Route {name: $route_ref})
			RETURN r.id, r.name, r.distance, r.road_type, r.max_speed, r.gradient
		`
		params["route_ref"] = routeRef

	case "filter_by":
		cypher = buildFilterQuery(params)

	default:
		return nil, fmt.Errorf("unsupported neo4j query_type: %s", queryType)
	}

	result, err := session.Run(ctx, cypher, params)
	if err != nil {
		return nil, err
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func buildFilterQuery(params map[string]interface{}) string {
	conditions := []string{}
	if roadType, ok := params["road_type"].(string); ok {
		conditions = append(conditions, fmt.Sprintf("r.road_type = '%s'", roadType))
	}
	if maxGradient, ok := params["max_gradient"].(float64); ok {
		conditions = append(conditions, fmt.Sprintf("r.gradient <= %f", maxGradient))
	}
	if maxSpeed, ok := params["max_speed"].(float64); ok {
		conditions = append(conditions, fmt.Sprintf("r.max_speed <= %f", maxSpeed))
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	return fmt.Sprintf(`
		MATCH (from:Node)-[r:ROAD]->(to:Node)
		%s
		RETURN r.id, from.name, to.name, r.distance, r.road_type, r.max_speed, r.gradient
		LIMIT 50
	`, whereClause)
}

func publishToSNS(ctx context.Context, requestID string, result interface{}) {
	message := map[string]interface{}{
		"request_id": requestID,
		"result":     result,
	}

	body, err := json.Marshal(message)
	if err != nil {
		log.Printf("Failed to marshal message: %v", err)
		return
	}

	_, err = snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(snsTopicARN),
		Message:  aws.String(string(body)),
	})

	if err != nil {
		log.Printf("Failed to publish to SNS: %v", err)
	} else {
		log.Printf("Published result to SNS for request: %s", requestID)
	}
}
