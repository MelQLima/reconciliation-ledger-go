package httpapi_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MelQLima/conciliacao-ledger-go/internal/httpapi"
	"github.com/MelQLima/conciliacao-ledger-go/internal/idempotency"
	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
	"github.com/MelQLima/conciliacao-ledger-go/internal/reconcile"
	"github.com/MelQLima/conciliacao-ledger-go/internal/store"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	svc := ledger.NewService(store.NewMemory(), idempotency.NewMemory())
	h := httpapi.New(svc, reconcile.New(svc, 4), slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

const settled = `{"event_id":"evt-1","type":"pix.settled","end_to_end_id":"E2E1",
"payer_account":"customer-1","payee_account":"merchant-1","amount_cents":15990,
"occurred_at":"2026-11-16T10:00:00Z"}`

func post(t *testing.T, url, contentType, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, contentType, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestEventsEndpoint(t *testing.T) {
	srv := newServer(t)

	tests := []struct {
		name        string
		body        string
		wantStatus  int
		wantProblem bool
	}{
		{"first delivery is recorded", settled, http.StatusCreated, false},
		{"replay is idempotent", settled, http.StatusOK, false},
		{"malformed json", `{"event_id":`, http.StatusBadRequest, true},
		{"unknown field", `{"foo":"bar"}`, http.StatusBadRequest, true},
		{"business validation", strings.Replace(strings.Replace(settled, "15990", "-1", 1), "evt-1", "evt-2", 1), http.StatusUnprocessableEntity, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := post(t, srv.URL+"/v1/events", "application/json", tt.body)
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if got := resp.Header.Get("Content-Type") == "application/problem+json"; got != tt.wantProblem {
				t.Fatalf("problem+json = %v, want %v", got, tt.wantProblem)
			}
		})
	}

	resp, err := http.Get(srv.URL + "/v1/accounts/merchant-1/balance")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		BalanceCents int64 `json:"balance_cents"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.BalanceCents != 15990 {
		t.Fatalf("balance = %d, want 15990 (replay must not double count)", body.BalanceCents)
	}
}

func TestBalanceNotFound(t *testing.T) {
	srv := newServer(t)
	resp, err := http.Get(srv.URL + "/v1/accounts/ghost/balance")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestReconciliationEndpoint(t *testing.T) {
	srv := newServer(t)
	post(t, srv.URL+"/v1/events", "application/json", settled)

	csv := "end_to_end_id,amount_cents,settled_at\nE2E1,15990,2026-11-16T10:00:02Z\nE2E-X,100,2026-11-16T11:00:00Z\n"
	resp := post(t, srv.URL+"/v1/reconciliations?date=2026-11-16", "text/csv", csv)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var rep reconcile.Report
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		t.Fatal(err)
	}
	if rep.Matched != 1 || len(rep.Divergences) != 1 || rep.Divergences[0].Kind != "missing_in_ledger" {
		t.Fatalf("unexpected report %+v", rep)
	}

	if r := post(t, srv.URL+"/v1/reconciliations?date=16-11-2026", "text/csv", csv); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad date status = %d", r.StatusCode)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	srv := newServer(t)
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", path, resp.StatusCode)
		}
	}
}
