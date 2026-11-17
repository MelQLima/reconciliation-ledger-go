// Package httpapi exposes the ledger over HTTP (chi router) with RFC 7807
// error responses, Prometheus metrics and OpenTelemetry tracing.
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
	"github.com/MelQLima/conciliacao-ledger-go/internal/observability"
	"github.com/MelQLima/conciliacao-ledger-go/internal/reconcile"
)

// Server holds the HTTP dependencies.
type Server struct {
	svc        *ledger.Service
	reconciler *reconcile.Reconciler
	log        *slog.Logger
}

// New builds the HTTP handler.
func New(svc *ledger.Service, rec *reconcile.Reconciler, log *slog.Logger) http.Handler {
	s := &Server{svc: svc, reconciler: rec, log: log}
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Recoverer, s.metrics)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/readyz", s.ready)
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Post("/events", s.postEvent)
		r.Get("/accounts/{accountID}/balance", s.getBalance)
		r.Post("/reconciliations", s.postReconciliation)
	})
	return otelhttp.NewHandler(r, "ledger-api")
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.Ready(r.Context()); err != nil {
		problem(w, http.StatusServiceUnavailable, "not ready", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// postEvent applies a payment event. Returns 201 when recorded and 200 when
// the event was a duplicate (idempotent replay).
func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {
	var ev ledger.PaymentEvent
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ev); err != nil {
		problem(w, http.StatusBadRequest, "invalid JSON", err.Error())
		return
	}
	res, err := s.svc.Apply(r.Context(), ev)
	switch {
	case errors.Is(err, ledger.ErrInvalidEvent):
		observability.EventsProcessed.WithLabelValues(string(ledger.ResultInvalid)).Inc()
		problem(w, http.StatusUnprocessableEntity, "invalid payment event", err.Error())
		return
	case err != nil:
		s.log.ErrorContext(r.Context(), "apply event", "error", err)
		problem(w, http.StatusInternalServerError, "internal error", "could not record event")
		return
	}
	observability.EventsProcessed.WithLabelValues(string(res)).Inc()
	status := http.StatusCreated
	if res == ledger.ResultDuplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]string{"event_id": ev.EventID, "result": string(res)})
}

func (s *Server) getBalance(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "accountID")
	b, err := s.svc.Balance(r.Context(), id)
	if errors.Is(err, ledger.ErrUnknownAccount) {
		problem(w, http.StatusNotFound, "account not found", id)
		return
	}
	if err != nil {
		problem(w, http.StatusInternalServerError, "internal error", "could not read balance")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"account_id": id, "balance_cents": b})
}

// postReconciliation accepts a CSV bank statement (text/csv) and ?date=YYYY-MM-DD.
func (s *Server) postReconciliation(w http.ResponseWriter, r *http.Request) {
	day, err := time.Parse("2006-01-02", r.URL.Query().Get("date"))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid date", "use ?date=YYYY-MM-DD")
		return
	}
	lines, err := reconcile.ParseStatement(http.MaxBytesReader(w, r.Body, 10<<20))
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid statement", err.Error())
		return
	}
	rep, err := s.reconciler.Run(r.Context(), day, lines)
	if err != nil {
		problem(w, http.StatusInternalServerError, "reconciliation failed", err.Error())
		return
	}
	for _, d := range rep.Divergences {
		observability.ReconciliationDivergences.WithLabelValues(d.Kind).Inc()
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		observability.HTTPDuration.WithLabelValues(route, r.Method, strconv.Itoa(ww.Status())).Observe(time.Since(start).Seconds())
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// problem writes an RFC 7807 application/problem+json response.
func problem(w http.ResponseWriter, status int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":   "about:blank",
		"title":  title,
		"status": status,
		"detail": detail,
	})
}
