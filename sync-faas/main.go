package main

import (
	"context"
	"log"
	"time"
)

func main() {
	ctx := context.Background()

	for {
		processed, err := ConsumeOnce(ctx)
		if err != nil {
			log.Printf("consume error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		_ = processed
	}
}
