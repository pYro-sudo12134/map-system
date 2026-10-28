package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type amAlert struct {
	Status string            `json:"status"`
	Labels map[string]string `json:"labels"`
}

type amPayload struct {
	Status string    `json:"status"`
	Alerts []amAlert `json:"alerts"`
}

type rule struct {
	Deployment string
	OnFiring   int32
	OnResolved int32
}

var rules = map[string]rule{
	"SQSSyncQueueTooLong":  {Deployment: "faas-s", OnFiring: 10, OnResolved: 2},
	"SQSSyncQueueVeryLong": {Deployment: "faas-s", OnFiring: 20, OnResolved: 2},
	"SQSMapQueueTooLong":   {Deployment: "faas-m", OnFiring: 10, OnResolved: 2},
	"SQSMapQueueVeryLong":  {Deployment: "faas-m", OnFiring: 20, OnResolved: 2},
}

type Scaler interface {
	Scale(ctx context.Context, namespace, deployment string, replicas int32) error
}

type k8sScaler struct {
	cs kubernetes.Interface
}

func (s k8sScaler) Scale(ctx context.Context, namespace, deployment string, replicas int32) error {
	body, _ := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{"replicas": replicas},
	})
	_, err := s.cs.AppsV1().Deployments(namespace).Patch(
		ctx, deployment, types.MergePatchType, body, metav1.PatchOptions{},
	)
	return err
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func newHandler(s Scaler, namespace string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		var p amPayload
		if err := json.Unmarshal(body, &p); err != nil {
			log.Printf("bad payload: %v", err)
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}

		targets := map[string]int32{}
		for _, a := range p.Alerts {
			name := a.Labels["alertname"]
			rl, ok := rules[name]
			if !ok {
				continue
			}
			var t int32
			switch a.Status {
			case "firing":
				t = rl.OnFiring
			case "resolved":
				t = rl.OnResolved
			default:
				continue
			}
			if prev, ok := targets[rl.Deployment]; !ok || t > prev {
				targets[rl.Deployment] = t
			}
		}

		for deploy, t := range targets {
			ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
			err := s.Scale(ctx, namespace, deploy, t)
			cancel()
			if err != nil {
				log.Printf("scale %s to %d: %v", deploy, t, err)
				http.Error(w, "scale error", http.StatusInternalServerError)
				return
			}
			log.Printf("scaled %s to %d", deploy, t)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

func main() {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("in-cluster config: %v", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("k8s client: %v", err)
	}
	ns := getenv("TARGET_NAMESPACE", "openfaas-fn")

	mux := http.NewServeMux()
	mux.HandleFunc("/scale", newHandler(k8sScaler{cs}, ns))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	addr := ":" + getenv("PORT", "8080")
	log.Printf("scale-adapter listening on %s, namespace=%s", addr, ns)
	log.Fatal(http.ListenAndServe(addr, mux))
}