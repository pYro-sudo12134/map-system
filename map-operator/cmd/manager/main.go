package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pYro-sudo12134/map-operator/map-system/internal/aws"
	"github.com/pYro-sudo12134/map-operator/map-system/internal/collector"
	"github.com/pYro-sudo12134/map-operator/map-system/internal/metrics"
)

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}

type metricsExporter struct{}

func (m *metricsExporter) SetQueueLength(queueName string, length int32) {
	metrics.SetQueueLength(queueName, length)
}

func (m *metricsExporter) IncScrapeErrors(queueName string) {
	metrics.IncScrapeErrors(queueName)
}

func (m *metricsExporter) SetLastScrapeTimestamp(queueName string, timestamp float64) {
	metrics.SetLastScrapeTimestamp(queueName, timestamp)
}

func main() {
	log.Println("Starting Go Operator for ISM (Metrics Exporter)")

	queueURL := getEnv("SQS_GO_URL", "http://localstack:4566/000000000000/SQS_GO")
	queueName := getEnv("QUEUE_NAME", "SQS_GO")
	checkInterval := getEnvDuration("CHECK_INTERVAL", 10*time.Second)
	metricsPort := getEnv("METRICS_PORT", "9090")

	ctx := context.Background()
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		awsCfg.BaseEndpoint = &endpoint
		log.Printf("Using custom AWS endpoint: %s", endpoint)
	}

	sqsReader := aws.NewSQSReader(awsCfg, queueURL)

	collectorConfig := collector.Config{
		QueueName:     queueName,
		CheckInterval: checkInterval,
	}

	exporter := &metricsExporter{}

	col := collector.NewCollector(
		collectorConfig,
		sqsReader,
		exporter,
	)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics endpoint listening on :%s", metricsPort)
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	col.Start()

	select {}
}
