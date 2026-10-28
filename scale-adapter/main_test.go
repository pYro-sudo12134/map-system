package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

const testNS = "openfaas-fn"

func newFake(t *testing.T) (*fake.Clientset, Scaler) {
	t.Helper()
	cs := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "faas-s", Namespace: testNS},
			Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(int32(2))},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "faas-m", Namespace: testNS},
			Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(int32(2))},
		},
	)
	return cs, k8sScaler{cs}
}

func replicas(t *testing.T, cs *fake.Clientset, name string) int32 {
	t.Helper()
	d, err := cs.AppsV1().Deployments(testNS).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	if d.Spec.Replicas == nil {
		return -1
	}
	return *d.Spec.Replicas
}

func post(t *testing.T, h http.HandlerFunc, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/scale", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func alertPayload(status, alertname, severity string) string {
	return `{"status":"` + status + `","alerts":[{"status":"` + status +
		`","labels":{"alertname":"` + alertname + `","severity":"` + severity + `"}}]}`
}

func TestHandler_TargetSelection(t *testing.T) {
	cases := []struct {
		name       string
		status     string
		alertname  string
		severity   string
		wantDeploy string
		wantRepl   int32
		wantCode   int
	}{
		{"firing sync TooLong", "firing", "SQSSyncQueueTooLong", "warning", "faas-s", 10, 200},
		{"firing sync VeryLong", "firing", "SQSSyncQueueVeryLong", "critical", "faas-s", 20, 200},
		{"firing map TooLong", "firing", "SQSMapQueueTooLong", "warning", "faas-m", 10, 200},
		{"firing map VeryLong", "firing", "SQSMapQueueVeryLong", "critical", "faas-m", 20, 200},
		{"resolved sync TooLong", "resolved", "SQSSyncQueueTooLong", "", "faas-s", 2, 200},
		{"resolved sync VeryLong", "resolved", "SQSSyncQueueVeryLong", "", "faas-s", 2, 200},
		{"resolved map TooLong", "resolved", "SQSMapQueueTooLong", "", "faas-m", 2, 200},
		{"resolved map VeryLong", "resolved", "SQSMapQueueVeryLong", "", "faas-m", 2, 200},
		{"unknown alert firing", "firing", "SomethingElse", "warning", "", 0, 200},
		{"unknown alert resolved", "resolved", "SomethingElse", "", "", 0, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs, s := newFake(t)
			h := newHandler(s, testNS)

			rec := post(t, h, alertPayload(tc.status, tc.alertname, tc.severity))
			if rec.Code != tc.wantCode {
				t.Fatalf("code=%d, want %d", rec.Code, tc.wantCode)
			}
			if tc.wantDeploy == "" {
				if got := replicas(t, cs, "faas-s"); got != 2 {
					t.Fatalf("faas-s changed to %d, want untouched 2", got)
				}
				if got := replicas(t, cs, "faas-m"); got != 2 {
					t.Fatalf("faas-m changed to %d, want untouched 2", got)
				}
				return
			}
			if got := replicas(t, cs, tc.wantDeploy); got != tc.wantRepl {
				t.Fatalf("%s replicas=%d, want %d", tc.wantDeploy, got, tc.wantRepl)
			}
		})
	}
}

func TestHandler_MultipleAlerts_MaxWins(t *testing.T) {
	cs, s := newFake(t)
	h := newHandler(s, testNS)

	body := `{"status":"firing","alerts":[
		{"status":"firing","labels":{"alertname":"SQSMapQueueVeryLong","severity":"critical"}},
		{"status":"firing","labels":{"alertname":"SQSMapQueueTooLong","severity":"warning"}}
	]}`
	if rec := post(t, h, body); rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	if got := replicas(t, cs, "faas-m"); got != 20 {
		t.Fatalf("faas-m replicas=%d, want 20 (max)", got)
	}
}

func TestHandler_MultipleAlerts_MaxWins_Reversed(t *testing.T) {
	cs, s := newFake(t)
	h := newHandler(s, testNS)

	body := `{"status":"firing","alerts":[
		{"status":"firing","labels":{"alertname":"SQSMapQueueTooLong","severity":"warning"}},
		{"status":"firing","labels":{"alertname":"SQSMapQueueVeryLong","severity":"critical"}}
	]}`
	if rec := post(t, h, body); rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	if got := replicas(t, cs, "faas-m"); got != 20 {
		t.Fatalf("faas-m replicas=%d, want 20 (max)", got)
	}
}

func TestHandler_FiringAndResolvedSameDeployment(t *testing.T) {
	cs, s := newFake(t)
	h := newHandler(s, testNS)

	body := `{"status":"firing","alerts":[
		{"status":"firing","labels":{"alertname":"SQSSyncQueueTooLong","severity":"warning"}},
		{"status":"resolved","labels":{"alertname":"SQSSyncQueueVeryLong","severity":"critical"}}
	]}`
	if rec := post(t, h, body); rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	if got := replicas(t, cs, "faas-s"); got != 10 {
		t.Fatalf("faas-s replicas=%d, want 10 (firing max)", got)
	}
}

func TestHandler_MethodAndBodyValidation(t *testing.T) {
	_, s := newFake(t)
	h := newHandler(s, testNS)

	t.Run("GET -> 405", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/scale", nil)
		rec := httptest.NewRecorder()
		h(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	t.Run("bad json -> 400", func(t *testing.T) {
		rec := post(t, h, `{not json`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("code=%d", rec.Code)
		}
	})
}

func TestHandler_ScaleError_Returns500(t *testing.T) {
	cs, s := newFake(t)
	cs.PrependReactor("patch", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.DeadlineExceeded
	})
	h := newHandler(s, testNS)

	rec := post(t, h, alertPayload("firing", "SQSSyncQueueTooLong", "warning"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code=%d, want 500", rec.Code)
	}
}

func TestK8sScaler_PatchesReplicasOnly(t *testing.T) {
	cs := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "faas-m", Namespace: testNS},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(2)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "faas-m"}},
		},
	})
	if err := (k8sScaler{cs}).Scale(context.Background(), testNS, "faas-m", 20); err != nil {
		t.Fatal(err)
	}
	d, _ := cs.AppsV1().Deployments(testNS).Get(context.Background(), "faas-m", metav1.GetOptions{})
	if *d.Spec.Replicas != 20 {
		t.Fatalf("replicas=%d", *d.Spec.Replicas)
	}
	if d.Spec.Selector == nil || d.Spec.Selector.MatchLabels["app"] != "faas-m" {
		t.Fatal("selector was clobbered by patch")
	}
}

func TestHandler_AlertmanagerPayloadShape(t *testing.T) {
	cs, s := newFake(t)
	h := newHandler(s, testNS)

	body := `{
		"version":"4",
		"groupKey":"{}:{alertname=\"SQSSyncQueueTooLong\"}",
		"status":"firing",
		"receiver":"scale-adapter",
		"groupLabels":{"alertname":"SQSSyncQueueTooLong"},
		"commonLabels":{"alertname":"SQSSyncQueueTooLong","severity":"warning"},
		"alerts":[{
			"status":"firing",
			"labels":{"alertname":"SQSSyncQueueTooLong","severity":"warning"},
			"annotations":{"summary":"x"},
			"startsAt":"2026-10-28T15:44:03.912Z",
			"endsAt":"0001-01-01T00:00:00Z",
			"generatorURL":"http://x"
		}]
	}`
	rec := post(t, h, body)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := replicas(t, cs, "faas-s"); got != 10 {
		t.Fatalf("faas-s replicas=%d, want 10", got)
	}
}
