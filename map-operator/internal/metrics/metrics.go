package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	QueueLength = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sqs_map_queue_length",
			Help: "Current number of messages in SQS_GO queue",
		},
		[]string{"queue_name"},
	)

	ScrapeErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sqs_map_scrape_errors_total",
			Help: "Total number of errors while scraping SQS_GO",
		},
		[]string{"queue_name"},
	)

	LastScrapeTimestamp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sqs_map_last_scrape_timestamp",
			Help: "Unix timestamp of last successful SQS_GO scrape",
		},
		[]string{"queue_name"},
	)
)

func SetQueueLength(queueName string, length int32) {
	QueueLength.WithLabelValues(queueName).Set(float64(length))
}

func IncScrapeErrors(queueName string) {
	ScrapeErrors.WithLabelValues(queueName).Inc()
}

func SetLastScrapeTimestamp(queueName string, timestamp float64) {
	LastScrapeTimestamp.WithLabelValues(queueName).Set(timestamp)
}
