package collector

import (
	"context"
	"log"
	"sync"
	"time"
)

type QueueReader interface {
	GetQueueLength(ctx context.Context) (int32, error)
}

type MetricsExporter interface {
	SetQueueLength(queueName string, length int32)
	IncScrapeErrors(queueName string)
	SetLastScrapeTimestamp(queueName string, timestamp float64)
}

type Config struct {
	QueueName     string
	CheckInterval time.Duration
}

type Collector struct {
	config   Config
	reader   QueueReader
	metrics  MetricsExporter
	stopOnce sync.Once
	stopChan chan struct{}
	doneChan chan struct{}
}

func NewCollector(config Config, reader QueueReader, metrics MetricsExporter) *Collector {
	return &Collector{
		config:   config,
		reader:   reader,
		metrics:  metrics,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
	}
}

func (c *Collector) Start(ctx context.Context) {
	log.Printf("Starting collector for queue: %s (interval: %v)",
		c.config.QueueName, c.config.CheckInterval)

	c.collect(ctx)

	go func() {
		defer close(c.doneChan)
		ticker := time.NewTicker(c.config.CheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.collect(ctx)
			case <-c.stopChan:
				log.Printf("Collector stopped for queue: %s", c.config.QueueName)
				return
			case <-ctx.Done():
				log.Printf("Collector context cancelled for queue: %s", c.config.QueueName)
				return
			}
		}
	}()
}

func (c *Collector) Stop() {
	c.stopOnce.Do(func() { close(c.stopChan) })
}

func (c *Collector) Wait() {
	select {
	case <-c.doneChan:
	case <-time.After(5 * time.Second):
	}
}

func (c *Collector) collect(ctx context.Context) {
	length, err := c.reader.GetQueueLength(ctx)

	if err != nil {
		c.metrics.IncScrapeErrors(c.config.QueueName)
		log.Printf("Failed to collect queue length for %s: %v", c.config.QueueName, err)
		return
	}

	c.metrics.SetQueueLength(c.config.QueueName, length)
	c.metrics.SetLastScrapeTimestamp(c.config.QueueName, float64(time.Now().Unix()))
	log.Printf("Collected queue length: %s = %d", c.config.QueueName, length)
}
