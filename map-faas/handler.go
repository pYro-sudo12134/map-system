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
	Result    interface{} `json:"result,omitempty"`
	Error     string      `json:"error,omitempty"`
}

var (
	neo4jDriver neo4j.DriverWithContext
	osClient    *opensearch.Client
	snsClient   *sns.Client
	snsTopicARN string
	nodesIndex  string
)

func SetNeo4jDriver(d neo4j.DriverWithContext) { neo4jDriver = d }
func SetOSClient(c *opensearch.Client)         { osClient = c }
func SetSNSClient(c *sns.Client)               { snsClient = c }
func SetSNSTopicARN(arn string)                { snsTopicARN = arn }
func SetNodesIndex(idx string)                 { nodesIndex = idx }

func init() {
	dbURI := os.Getenv("NEO4J_URI")
	dbUser := os.Getenv("NEO4J_USER")
	dbPassword := os.Getenv("NEO4J_PASSWORD")

	if dbURI != "" {
		var err error
		neo4jDriver, err = neo4j.NewDriverWithContext(dbURI, neo4j.BasicAuth(dbUser, dbPassword, ""))
		if err != nil {
			log.Printf("Failed to create Neo4j driver: %v", err)
		}
	}

	osURL := os.Getenv("OPENSEARCH_URL")
	if osURL == "" {
		log.Println("OPENSEARCH_URL not set, OpenSearch features disabled")
	} else {
		var err error
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
	awsEndpoint := os.Getenv("AWS_ENDPOINT_URL")

	if awsEndpoint != "" || snsTopicARN != "" {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			log.Printf("Failed to load AWS config: %v", err)
		} else {
			var opts []func(*sns.Options)

			if awsEndpoint != "" {
				cfg.Credentials = aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
					return aws.Credentials{
						AccessKeyID:     "test",
						SecretAccessKey: "test",
						Source:          "static",
					}, nil
				})
				if cfg.Region == "" {
					cfg.Region = "us-east-1"
				}
				opts = append(opts, func(o *sns.Options) {
					o.BaseEndpoint = aws.String(awsEndpoint)
				})
			}

			snsClient = sns.NewFromConfig(cfg, opts...)
		}
	}
}

// Handle executes a single map request. On success it publishes the result
// to the SNS topic and returns {"request_id":..., "result":...}. On error
// it returns {"request_id":..., "error":"..."} and does NOT publish. The
// caller (ConsumeOnce) leaves the SQS message for redelivery on failure.
func Handle(ctx context.Context, req []byte) (string, error) {
	var request Request
	if err := json.Unmarshal(req, &request); err != nil {
		log.Printf("Failed to unmarshal request: %v", err)
		return "", err
	}

	log.Printf("Processing request: %s", request.RequestID)

	queryType, ok := request.ParsedQuery["query_type"].(string)
	if !ok {
		return failedResponse(request.RequestID, "missing query_type"), nil
	}

	params, ok := request.ParsedQuery["params"].(map[string]interface{})
	if !ok {
		return failedResponse(request.RequestID, "missing params"), nil
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
		if neo4jDriver == nil {
			err = fmt.Errorf("Neo4j not available for %s queries", queryType)
		} else {
			result, err = executeNeo4jQuery(ctx, queryType, params)
		}
	default:
		err = fmt.Errorf("unsupported query_type: %s", queryType)
	}

	if err != nil {
		log.Printf("Request %s failed: %v", request.RequestID, err)
		return failedResponse(request.RequestID, err.Error()), nil
	}

	publishToSNS(ctx, request.RequestID, result)

	return okResponse(request.RequestID, result), nil
}

func okResponse(requestID string, result interface{}) string {
	b, err := json.Marshal(Response{
		RequestID: requestID,
		Result:    result,
	})
	if err != nil {
		return `{"error":"marshal error"}`
	}
	return string(b)
}

func failedResponse(requestID, msg string) string {
	b, err := json.Marshal(Response{
		RequestID: requestID,
		Error:     msg,
	})
	if err != nil {
		return `{"error":"marshal error"}`
	}
	return string(b)
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
					if cerr := Body.Close(); cerr != nil {
						log.Printf("Error closing body: %v", cerr)
					}
				}(res.Body)
				var result map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&result); err == nil {
					hits, ok := result["hits"].(map[string]interface{})
					if ok {
						hitHits, ok := hits["hits"].([]interface{})
						if ok && len(hitHits) > 0 {
							if hit, ok := hitHits[0].(map[string]interface{}); ok {
								if source, ok := hit["_source"].(map[string]interface{}); ok {
									latF, ok1 := source["lat"].(float64)
									lonF, ok2 := source["lon"].(float64)
									if ok1 && ok2 {
										log.Printf("Found location '%s' -> (%f, %f)", locationRef, latF, lonF)
										return latF, lonF, nil
									}
								}
							}
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

	if locationRef, ok := params["location_ref"].(string); ok && lat == 0 && lon == 0 {
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
		if cerr := Body.Close(); cerr != nil {
			log.Printf("Error closing body: %v", cerr)
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
		if cerr := Body.Close(); cerr != nil {
			log.Printf("Error closing body: %v", cerr)
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
	defer func() {
		if err := session.Close(ctx); err != nil {
			log.Printf("Error closing session: %v", err)
		}
	}()

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
	if roadType, ok := params["road_type"].(string); ok && roadType != "" {
		conditions = append(conditions, "r.road_type = $road_type")
		params["road_type"] = roadType
	}
	if maxGradient, ok := params["max_gradient"].(float64); ok {
		conditions = append(conditions, "r.gradient <= $max_gradient")
		params["max_gradient"] = maxGradient
	}
	if maxSpeed, ok := params["max_speed"].(float64); ok {
		conditions = append(conditions, "r.max_speed <= $max_speed")
		params["max_speed"] = maxSpeed
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
	if snsClient == nil || snsTopicARN == "" {
		log.Printf("SNS not configured, skipping publish for request %s", requestID)
		return
	}

	message := map[string]interface{}{
		"request_id": requestID,
		"result":     result,
	}

	body, err := json.Marshal(message)
	if err != nil {
		log.Printf("Failed to marshal SNS message: %v", err)
		return
	}

	if _, err := snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(snsTopicARN),
		Message:  aws.String(string(body)),
	}); err != nil {
		log.Printf("Failed to publish to SNS: %v", err)
	} else {
		log.Printf("Published result to SNS for request: %s", requestID)
	}
}
