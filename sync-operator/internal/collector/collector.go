package collector

import (
	"log"
	"time"
)

type QueueReader interface {
	GetQueueLength() (int32, error)
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
	stopChan chan struct{}
}

func NewCollector(
	config Config,
	reader QueueReader,
	metrics MetricsExporter,
) *Collector {
	return &Collector{
		config:   config,
		reader:   reader,
		metrics:  metrics,
		stopChan: make(chan struct{}),
	}
}

func (c *Collector) Start() {
	log.Printf("Starting collector for queue: %s (interval: %v)",
		c.config.QueueName, c.config.CheckInterval)

	ticker := time.NewTicker(c.config.CheckInterval)

	c.collect()

	go func() {
		for {
			select {
			case <-ticker.C:
				c.collect()
			case <-c.stopChan:
				ticker.Stop()
				log.Printf("Collector stopped for queue: %s", c.config.QueueName)
				return
			}
		}
	}()
}

func (c *Collector) Stop() {
	close(c.stopChan)
}

func (c *Collector) collect() {
	length, err := c.reader.GetQueueLength()

	if err != nil {
		c.metrics.IncScrapeErrors(c.config.QueueName)
		log.Printf("Failed to collect queue length: %v", err)
		return
	}

	c.metrics.SetQueueLength(c.config.QueueName, length)
	c.metrics.SetLastScrapeTimestamp(c.config.QueueName, float64(time.Now().Unix()))
	log.Printf("Collected queue length: %s = %d", c.config.QueueName, length)
}
