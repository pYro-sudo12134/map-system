package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pYro-sudo12134/map-operator/map-system/internal/aws"
	"github.com/pYro-sudo12134/map-operator/map-system/internal/collector"
	"github.com/pYro-sudo12134/map-operator/map-system/internal/metrics"
)

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
	log.Println("Starting Map Operator for ISM (Metrics Exporter)")

	queueURL := getEnv("SQS_GO_URL", "")
	queueName := getEnv("QUEUE_NAME", "SQS_GO")
	checkInterval := getEnvDuration("CHECK_INTERVAL", 10*time.Second)
	metricsPort := getEnv("METRICS_PORT", "9090")
	awsEndpoint := os.Getenv("AWS_ENDPOINT_URL")

	if queueURL == "" {
		log.Fatal("SQS_GO_URL is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	sqsReader := aws.NewSQSReader(awsCfg, queueURL, awsEndpoint)

	col := collector.NewCollector(
		collector.Config{
			QueueName:     queueName,
			CheckInterval: checkInterval,
		},
		sqsReader,
		&metricsExporter{},
	)
	col.Start(ctx)

	srv := &http.Server{
		Addr:    ":" + metricsPort,
		Handler: promhttp.Handler(),
	}
	go func() {
		log.Printf("Metrics endpoint listening on :%s", metricsPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Metrics server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutdown signal received, stopping collector")

	col.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Metrics server shutdown error: %v", err)
	}

	col.Wait()
	log.Println("Shutdown complete")
}

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
		log.Printf("Invalid duration %q for %s, using default %v", value, key, defaultValue)
	}
	return defaultValue
}
