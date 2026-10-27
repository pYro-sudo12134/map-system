package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync/atomic"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v2"
	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
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
	neo4jDriver    neo4j.DriverWithContext
	osClient       *opensearch.Client
	nodesIndex     string
	edgesIndex     string
	indicesEnsured atomic.Bool
)

func SetNeo4jDriver(d neo4j.DriverWithContext) { neo4jDriver = d }
func SetOSClient(c *opensearch.Client)         { osClient = c }
func SetNodesIndex(idx string)                 { nodesIndex = idx }
func SetEdgesIndex(idx string)                 { edgesIndex = idx }
func ResetIndicesEnsured()                     { indicesEnsured.Store(false) }

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
	if osURL != "" {
		var err error
		osClient, err = opensearch.NewClient(opensearch.Config{
			Addresses: []string{osURL},
			Username:  os.Getenv("OPENSEARCH_USER"),
			Password:  os.Getenv("OPENSEARCH_PASSWORD"),
		})
		if err != nil {
			log.Printf("Failed to create OpenSearch client: %v", err)
		}
	}

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

func ensureIndices(ctx context.Context) error {
	if indicesEnsured.Load() {
		return nil
	}

	if err := createIndexIfMissing(ctx, nodesIndex, nodesMapping()); err != nil {
		return fmt.Errorf("ensure index %s: %w", nodesIndex, err)
	}
	if err := createIndexIfMissing(ctx, edgesIndex, edgesMapping()); err != nil {
		return fmt.Errorf("ensure index %s: %w", edgesIndex, err)
	}

	indicesEnsured.Store(true)
	log.Printf("Indices ensured: %s, %s", nodesIndex, edgesIndex)
	return nil
}

func createIndexIfMissing(ctx context.Context, index string, body map[string]interface{}) error {
	res, err := osClient.Indices.Exists(
		[]string{index},
		osClient.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("exists check: %w", err)
	}
	_ = res.Body.Close()

	switch res.StatusCode {
	case 200:
		return nil
	case 404:
		createRes, err := osClient.Indices.Create(
			index,
			osClient.Indices.Create.WithBody(mustJSONReader(body)),
			osClient.Indices.Create.WithContext(ctx),
		)
		if err != nil {
			return fmt.Errorf("create: %w", err)
		}
		defer func() {
			if cerr := createRes.Body.Close(); cerr != nil {
				log.Printf("Failed to close create-index body: %v", cerr)
			}
		}()
		if createRes.IsError() {
			return fmt.Errorf("create http error: %s", createRes.String())
		}
		return nil
	default:
		return fmt.Errorf("unexpected status from exists check: %d", res.StatusCode)
	}
}

func mustJSONReader(v interface{}) *bytes.Reader {
	b, err := json.Marshal(v)
	if err != nil {
		// Body is a static map literal; marshal failure is a programming error.
		panic(fmt.Sprintf("marshal index body: %v", err))
	}
	return bytes.NewReader(b)
}

// nodesMapping deliberately declares location as geo_point: faas-m issues
// geo_distance and _geo_distance queries against this field, and dynamic
// mapping would store it as a plain object, which those queries reject.
func nodesMapping() map[string]interface{} {
	return map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"id":       map[string]string{"type": "keyword"},
				"name":     map[string]string{"type": "text"},
				"lat":      map[string]string{"type": "float"},
				"lon":      map[string]string{"type": "float"},
				"type":     map[string]string{"type": "keyword"},
				"location": map[string]string{"type": "geo_point"},
			},
		},
	}
}

func edgesMapping() map[string]interface{} {
	return map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"id":           map[string]string{"type": "keyword"},
				"from_node_id": map[string]string{"type": "keyword"},
				"to_node_id":   map[string]string{"type": "keyword"},
				"distance":     map[string]string{"type": "float"},
				"road_type":    map[string]string{"type": "keyword"},
				"max_speed":    map[string]string{"type": "integer"},
				"gradient":     map[string]string{"type": "float"},
			},
		},
	}
}

func Handle(ctx context.Context, req []byte) (string, error) {
	var request SyncRequest
	if err := json.Unmarshal(req, &request); err != nil {
		log.Printf("Failed to unmarshal request: %v", err)
		return "", err
	}

	log.Printf("Processing sync: %s", request.SyncType)

	var syncErr error
	switch request.SyncType {
	case "full":
		syncErr = fullSync(ctx)
	case "nodes":
		syncErr = syncNodes(ctx, request.NodeIDs)
	case "edges":
		syncErr = syncEdges(ctx, request.EdgeIDs)
	default:
		return jsonString(map[string]interface{}{
			"status": "failed",
			"error":  "unknown sync_type",
		}), nil
	}

	if syncErr != nil {
		log.Printf("%s sync failed: %v", request.SyncType, syncErr)
		return jsonString(map[string]interface{}{
			"status": "failed",
			"error":  syncErr.Error(),
		}), nil
	}

	return jsonString(map[string]interface{}{"status": "synced"}), nil
}

func jsonString(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"status":"failed","error":"marshal error"}`
	}
	return string(b)
}

func fullSync(ctx context.Context) error {
	log.Println("Starting full sync")

	if err := syncNodes(ctx, nil); err != nil {
		return fmt.Errorf("nodes: %w", err)
	}

	if err := syncEdges(ctx, nil); err != nil {
		return fmt.Errorf("edges: %w", err)
	}

	log.Println("Full sync completed")
	return nil
}

func syncNodes(ctx context.Context, nodeIDs []string) error {
	if err := ensureIndices(ctx); err != nil {
		return err
	}

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

	total := 0
	batch := make([]Node, 0, 100)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bulkIndexNodes(ctx, batch); err != nil {
			return err
		}
		total += len(batch)
		batch = batch[:0]
		return nil
	}

	for result.Next(ctx) {
		node, ok := recordToNode(result.Record())
		if !ok {
			log.Printf("Skipping malformed node record: %v", result.Record().Values)
			continue
		}
		batch = append(batch, node)
		if len(batch) >= 100 {
			if err := flush(); err != nil {
				return fmt.Errorf("bulk index nodes: %w", err)
			}
		}
	}

	if err := result.Err(); err != nil {
		return err
	}

	if err := flush(); err != nil {
		return fmt.Errorf("bulk index nodes: %w", err)
	}

	log.Printf("Synced %d nodes", total)
	return nil
}

func syncEdges(ctx context.Context, edgeIDs []string) error {
	if err := ensureIndices(ctx); err != nil {
		return err
	}

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

	total := 0
	batch := make([]Edge, 0, 100)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := bulkIndexEdges(ctx, batch); err != nil {
			return err
		}
		total += len(batch)
		batch = batch[:0]
		return nil
	}

	for result.Next(ctx) {
		edge, ok := recordToEdge(result.Record())
		if !ok {
			log.Printf("Skipping malformed edge record: %v", result.Record().Values)
			continue
		}
		batch = append(batch, edge)
		if len(batch) >= 100 {
			if err := flush(); err != nil {
				return fmt.Errorf("bulk index edges: %w", err)
			}
		}
	}

	if err := result.Err(); err != nil {
		return err
	}

	if err := flush(); err != nil {
		return fmt.Errorf("bulk index edges: %w", err)
	}

	log.Printf("Synced %d edges", total)
	return nil
}

func recordToNode(record *neo4j.Record) (Node, bool) {
	if len(record.Values) < 5 {
		return Node{}, false
	}
	id, ok := record.Values[0].(string)
	if !ok {
		return Node{}, false
	}
	name, _ := record.Values[1].(string)
	lat, okLat := numberToFloat(record.Values[2])
	lon, okLon := numberToFloat(record.Values[3])
	if !okLat || !okLon {
		return Node{}, false
	}
	typ, _ := record.Values[4].(string)
	return Node{ID: id, Name: name, Lat: lat, Lon: lon, Type: typ}, true
}

func recordToEdge(record *neo4j.Record) (Edge, bool) {
	if len(record.Values) < 7 {
		return Edge{}, false
	}
	id, ok := record.Values[0].(string)
	if !ok {
		return Edge{}, false
	}
	from, _ := record.Values[1].(string)
	to, _ := record.Values[2].(string)
	dist, okDist := numberToFloat(record.Values[3])
	if !okDist {
		return Edge{}, false
	}
	roadType, _ := record.Values[4].(string)
	speed, _ := numberToInt(record.Values[5])
	grad, _ := numberToFloat(record.Values[6])
	return Edge{
		ID:         id,
		FromNodeID: from,
		ToNodeID:   to,
		Distance:   dist,
		RoadType:   roadType,
		MaxSpeed:   speed,
		Gradient:   grad,
	}, true
}

func numberToFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	default:
		return 0, false
	}
}

func numberToInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case int32:
		return int(n), true
	case float64:
		return int(n), true
	case float32:
		return int(n), true
	default:
		return 0, false
	}
}

func buildNodesQuery(nodeIDs []string) (string, map[string]interface{}) {
	if len(nodeIDs) == 0 {
		return "MATCH (n:Node) RETURN n.id, n.name, n.lat, n.lon, n.type",
			map[string]interface{}{}
	}
	return "MATCH (n:Node) WHERE n.id IN $ids RETURN n.id, n.name, n.lat, n.lon, n.type",
		map[string]interface{}{"ids": nodeIDs}
}

func buildEdgesQuery(edgeIDs []string) (string, map[string]interface{}) {
	if len(edgeIDs) == 0 {
		return `MATCH (from)-[r:ROAD]->(to)
			RETURN r.id, from.id, to.id, r.distance, r.road_type, r.max_speed, r.gradient`,
			map[string]interface{}{}
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
		action, err := json.Marshal(map[string]interface{}{
			"index": map[string]interface{}{"_id": node.ID},
		})
		if err != nil {
			return err
		}
		body, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		buf.Write(action)
		buf.WriteByte('\n')
		buf.Write(body)
		buf.WriteByte('\n')
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

	return checkBulkResponse(res, "nodes")
}

func bulkIndexEdges(ctx context.Context, edges []Edge) error {
	if len(edges) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, edge := range edges {
		action, err := json.Marshal(map[string]interface{}{
			"index": map[string]interface{}{"_id": edge.ID},
		})
		if err != nil {
			return err
		}
		body, err := json.Marshal(edge)
		if err != nil {
			return err
		}
		buf.Write(action)
		buf.WriteByte('\n')
		buf.Write(body)
		buf.WriteByte('\n')
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

	return checkBulkResponse(res, "edges")
}

func checkBulkResponse(res *opensearchapi.Response, kind string) error {
	if res.IsError() {
		return fmt.Errorf("bulk index %s http error: %s", kind, res.String())
	}

	var br struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.NewDecoder(res.Body).Decode(&br); err != nil {
		return fmt.Errorf("decode bulk response %s: %w", kind, err)
	}
	if !br.Errors {
		return nil
	}

	var firstErr string
	for _, item := range br.Items {
		for _, op := range item {
			if op.Status >= 300 {
				firstErr = fmt.Sprintf("%s: %s", op.Error.Type, op.Error.Reason)
				break
			}
		}
		if firstErr != "" {
			break
		}
	}
	if firstErr == "" {
		firstErr = "unknown per-document error"
	}
	return fmt.Errorf("bulk index %s had errors: %s", kind, firstErr)
}
