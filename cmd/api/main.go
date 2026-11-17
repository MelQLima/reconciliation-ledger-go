// Command api runs the ledger HTTP API and, when Kafka is configured, the
// payment events consumer in the same process.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"

	"github.com/MelQLima/conciliacao-ledger-go/internal/config"
	"github.com/MelQLima/conciliacao-ledger-go/internal/consumer"
	"github.com/MelQLima/conciliacao-ledger-go/internal/httpapi"
	"github.com/MelQLima/conciliacao-ledger-go/internal/idempotency"
	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
	"github.com/MelQLima/conciliacao-ledger-go/internal/observability"
	"github.com/MelQLima/conciliacao-ledger-go/internal/reconcile"
	"github.com/MelQLima/conciliacao-ledger-go/internal/store"
)

func main() {
	cfg := config.Load()
	log := observability.NewLogger(cfg.LogLevel)
	if err := run(cfg, log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, "conciliacao-ledger")
	if err != nil {
		return err
	}

	var st ledger.Store = store.NewMemory()
	if cfg.DatabaseURL != "" {
		pg, err := store.NewPostgres(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pg.Close()
		st = pg
		log.Info("using PostgreSQL store")
	} else {
		log.Warn("DATABASE_URL not set: using in-memory store")
	}

	var guard ledger.Guard
	if cfg.RedisAddr != "" {
		r := idempotency.NewRedis(cfg.RedisAddr)
		defer r.Close()
		guard = r
	}

	svc := ledger.NewService(st, guard)
	rec := reconcile.New(svc, cfg.Workers)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.New(svc, rec, log),
		ReadHeaderTimeout: 5 * time.Second,
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	if len(cfg.KafkaBrokers) > 0 {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: cfg.KafkaBrokers,
			Topic:   cfg.KafkaTopic,
			GroupID: cfg.KafkaGroupID,
		})
		dlq := &kafka.Writer{Addr: kafka.TCP(cfg.KafkaBrokers...), Topic: cfg.KafkaDLQTopic, RequiredAcks: kafka.RequireAll}
		defer reader.Close()
		defer dlq.Close()
		c := consumer.New(reader, dlq, svc, log, consumer.Options{OnResult: func(r string) { observability.EventsProcessed.WithLabelValues(r).Inc() }})
		g.Go(func() error {
			log.Info("kafka consumer started", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
			return c.Run(gctx)
		})
	}

	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		log.Info("shutting down")
		_ = shutdownTracing(sctx)
		return srv.Shutdown(sctx)
	})

	return g.Wait()
}
