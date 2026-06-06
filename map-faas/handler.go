package function

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Request struct {
	RequestID   string                 `json:"request_id"`
	ParsedQuery map[string]interface{} `json:"parsed_query"`
}

type Response struct {
	RequestID string      `json:"request_id"`
	Result    interface{} `json:"result"`
	Error     string      `json:"error,omitempty"`
}

var (
	neo4jDriver neo4j.DriverWithContext
	snsClient   *sns.Client
	snsTopicARN string
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

	result, err := executeQuery(ctx, queryType, params)
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

func executeQuery(ctx context.Context, queryType string, params map[string]interface{}) (interface{}, error) {
	session := neo4jDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer func() {
		if err := session.Close(ctx); err != nil {
			log.Printf("Failed to close Neo4j session: %v", err)
		}
	}()

	var cypher string

	switch queryType {
	case "proximity":
		lat, ok := params["lat"].(float64)
		if !ok {
			return nil, fmt.Errorf("missing lat in params")
		}
		lon, ok := params["lon"].(float64)
		if !ok {
			return nil, fmt.Errorf("missing lon in params")
		}
		radius, ok := params["radius_km"].(float64)
		if !ok {
			return nil, fmt.Errorf("missing radius_km in params")
		}

		cypher = `
			MATCH (n:Node)
			WHERE distance(point({latitude: n.lat, longitude: n.lon}), point({latitude: $lat, longitude: $lon})) <= $radius * 1000
			RETURN n.id, n.name, n.lat, n.lon
			LIMIT 20
		`
		params["lat"] = lat
		params["lon"] = lon
		params["radius"] = radius

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
			MATCH (start:Node {name: $from_ref}), (end:Node {name: $to_ref})
			MATCH path = shortestPath((start)-[:ROAD*]-(end))
			RETURN 
				[n in nodes(path) | n.name] as path,
				reduce(total = 0, r in relationships(path) | total + r.distance) as total_distance
		`
		params["from_ref"] = fromRef
		params["to_ref"] = toRef

	default:
		return nil, fmt.Errorf("unsupported query_type: %s", queryType)
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
