package function

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v2"
)

type SyncRequest struct {
	SyncType string   `json:"sync_type"`
	NodeIDs  []string `json:"node_ids,omitempty"`
	EdgeIDs  []string `json:"edge_ids,omitempty"`
}

type Node struct {
	ID   string  `json:"id"`
	Name string  `json:"name"`
	Lat  float64 `json:"lat"`
	Lon  float64 `json:"lon"`
	Type string  `json:"type"`
}

type Edge struct {
	ID         string  `json:"id"`
	FromNodeID string  `json:"from_node_id"`
	ToNodeID   string  `json:"to_node_id"`
	Distance   float64 `json:"distance"`
	RoadType   string  `json:"road_type"`
	MaxSpeed   int     `json:"max_speed"`
	Gradient   float64 `json:"gradient"`
}

var (
	neo4jDriver neo4j.DriverWithContext
	osClient    *opensearch.Client
	sqsClient   *sqs.Client
	queueURL    string
	nodesIndex  string
	edgesIndex  string
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
	osClient, err = opensearch.NewClient(opensearch.Config{
		Addresses: []string{osURL},
		Username:  os.Getenv("OPENSEARCH_USER"),
		Password:  os.Getenv("OPENSEARCH_PASSWORD"),
	})
	if err != nil {
		log.Fatalf("Failed to create OpenSearch client: %v", err)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		cfg.BaseEndpoint = &endpoint
	}

	sqsClient = sqs.NewFromConfig(cfg)
	queueURL = os.Getenv("SQS_SYNC_RESULTS_URL")

	nodesIndex = os.Getenv("OPENSEARCH_NODES_INDEX")
	if nodesIndex == "" {
		nodesIndex = "nodes"
		log.Printf("OPENSEARCH_NODES_INDEX not set, using default: %s", nodesIndex)
	}

	edgesIndex = os.Getenv("OPENSEARCH_EDGES_INDEX")
	if edgesIndex == "" {
		edgesIndex = "edges"
		log.Printf("OPENSEARCH_EDGES_INDEX not set, using default: %s", edgesIndex)
	}
}

func Handle(ctx context.Context, req []byte) (string, error) {
	var request SyncRequest
	if err := json.Unmarshal(req, &request); err != nil {
		log.Printf("Failed to unmarshal request: %v", err)
		return "", err
	}

	log.Printf("Processing sync: %s", request.SyncType)

	switch request.SyncType {
	case "full":
		if err := fullSync(ctx); err != nil {
			log.Printf("Full sync failed: %v", err)
			return `{"status":"failed","error":"` + err.Error() + `"}`, nil
		}
	case "nodes":
		if err := syncNodes(ctx, request.NodeIDs); err != nil {
			log.Printf("Nodes sync failed: %v", err)
			return `{"status":"failed","error":"` + err.Error() + `"}`, nil
		}
	case "edges":
		if err := syncEdges(ctx, request.EdgeIDs); err != nil {
			log.Printf("Edges sync failed: %v", err)
			return `{"status":"failed","error":"` + err.Error() + `"}`, nil
		}
	default:
		return `{"status":"failed","error":"unknown sync_type"}`, nil
	}

	if queueURL != "" {
		sendNotification(request.SyncType)
	}

	return `{"status":"synced"}`, nil
}

func fullSync(ctx context.Context) error {
	log.Println("Starting full sync")

	if err := syncNodes(ctx, nil); err != nil {
		return err
	}

	if err := syncEdges(ctx, nil); err != nil {
		return err
	}

	log.Println("Full sync completed")
	return nil
}

func syncNodes(ctx context.Context, nodeIDs []string) error {
	session := neo4jDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer func() {
		if err := session.Close(ctx); err != nil {
			log.Printf("Failed to close Neo4j session: %v", err)
		}
	}()

	query, params := buildNodesQuery(nodeIDs)

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return err
	}

	count := 0
	batch := make([]Node, 0, 100)

	for result.Next(ctx) {
		record := result.Record()
		node := Node{
			ID:   record.Values[0].(string),
			Name: record.Values[1].(string),
			Lat:  record.Values[2].(float64),
			Lon:  record.Values[3].(float64),
			Type: record.Values[4].(string),
		}
		batch = append(batch, node)

		if len(batch) >= 100 {
			if err := bulkIndexNodes(ctx, batch); err != nil {
				log.Printf("Failed to bulk index nodes: %v", err)
			}
			batch = batch[:0]
			count += 100
		}
	}

	if len(batch) > 0 {
		if err := bulkIndexNodes(ctx, batch); err != nil {
			log.Printf("Failed to bulk index nodes: %v", err)
		}
		count += len(batch)
	}

	log.Printf("Synced %d nodes", count)
	return result.Err()
}

func syncEdges(ctx context.Context, edgeIDs []string) error {
	session := neo4jDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer func() {
		if err := session.Close(ctx); err != nil {
			log.Printf("Failed to close Neo4j session: %v", err)
		}
	}()

	query, params := buildEdgesQuery(edgeIDs)

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return err
	}

	count := 0
	batch := make([]Edge, 0, 100)

	for result.Next(ctx) {
		record := result.Record()
		edge := Edge{
			ID:         record.Values[0].(string),
			FromNodeID: record.Values[1].(string),
			ToNodeID:   record.Values[2].(string),
			Distance:   record.Values[3].(float64),
			RoadType:   record.Values[4].(string),
			MaxSpeed:   record.Values[5].(int),
			Gradient:   record.Values[6].(float64),
		}
		batch = append(batch, edge)

		if len(batch) >= 100 {
			if err := bulkIndexEdges(ctx, batch); err != nil {
				log.Printf("Failed to bulk index edges: %v", err)
			}
			batch = batch[:0]
			count += 100
		}
	}

	if len(batch) > 0 {
		if err := bulkIndexEdges(ctx, batch); err != nil {
			log.Printf("Failed to bulk index edges: %v", err)
		}
		count += len(batch)
	}

	log.Printf("Synced %d edges", count)
	return result.Err()
}

func buildNodesQuery(nodeIDs []string) (string, map[string]interface{}) {
	if len(nodeIDs) == 0 {
		return "MATCH (n:Node) RETURN n.id, n.name, n.lat, n.lon, n.type", nil
	}
	return "MATCH (n:Node) WHERE n.id IN $ids RETURN n.id, n.name, n.lat, n.lon, n.type",
		map[string]interface{}{"ids": nodeIDs}
}

func buildEdgesQuery(edgeIDs []string) (string, map[string]interface{}) {
	if len(edgeIDs) == 0 {
		return `MATCH (from)-[r:ROAD]->(to) 
			RETURN r.id, from.id, to.id, r.distance, r.road_type, r.max_speed, r.gradient`, nil
	}
	return `MATCH (from)-[r:ROAD]->(to) 
		WHERE r.id IN $ids 
		RETURN r.id, from.id, to.id, r.distance, r.road_type, r.max_speed, r.gradient`,
		map[string]interface{}{"ids": edgeIDs}
}

func bulkIndexNodes(ctx context.Context, nodes []Node) error {
	if len(nodes) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, node := range nodes {
		doc := map[string]interface{}{
			"id":       node.ID,
			"name":     node.Name,
			"lat":      node.Lat,
			"lon":      node.Lon,
			"type":     node.Type,
			"location": map[string]float64{"lat": node.Lat, "lon": node.Lon},
		}
		body, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		buf.WriteString(fmt.Sprintf(`{ "index" : { "_id" : "%s" } }%s`, node.ID, "\n"))
		buf.Write(body)
		buf.WriteString("\n")
	}

	res, err := osClient.Bulk(bytes.NewReader(buf.Bytes()),
		osClient.Bulk.WithIndex(nodesIndex),
		osClient.Bulk.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("Failed to close bulk response body: %v", err)
		}
	}()

	if res.IsError() {
		return fmt.Errorf("bulk index error: %s", res.String())
	}
	return nil
}

func bulkIndexEdges(ctx context.Context, edges []Edge) error {
	if len(edges) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, edge := range edges {
		body, _ := json.Marshal(edge)
		buf.WriteString(fmt.Sprintf(`{ "index" : { "_id" : "%s" } }%s`, edge.ID, "\n"))
		buf.Write(body)
		buf.WriteString("\n")
	}

	res, err := osClient.Bulk(bytes.NewReader(buf.Bytes()),
		osClient.Bulk.WithIndex(edgesIndex),
		osClient.Bulk.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("Failed to close bulk response body: %v", err)
		}
	}()

	if res.IsError() {
		return fmt.Errorf("bulk index error: %s", res.String())
	}
	return nil
}

func sendNotification(syncType string) {
	if queueURL == "" {
		return
	}

	message := map[string]interface{}{
		"sync_type": syncType,
		"status":    "completed",
	}

	body, err := json.Marshal(message)
	if err != nil {
		log.Printf("Failed to marshal notification: %v", err)
		return
	}

	_, err = sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		log.Printf("Failed to send notification: %v", err)
	}
}
