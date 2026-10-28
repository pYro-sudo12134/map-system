package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	go func() {
		for {
			if _, err := ConsumeOnce(context.Background()); err != nil {
				log.Printf("consume error: %v", err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	log.Printf("sync-faas fprocess listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
