// Package consumer reads payment events from Kafka and applies them to the
// ledger with retries, exponential backoff and a dead-letter topic.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/MelQLima/conciliacao-ledger-go/internal/ledger"
)

// Source abstracts kafka.Reader so the loop can be unit tested.
type Source interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Sink abstracts kafka.Writer (used for the dead-letter topic).
type Sink interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Applier is implemented by *ledger.Service.
type Applier interface {
	Apply(ctx context.Context, e ledger.PaymentEvent) (ledger.Result, error)
}

// Consumer processes messages one by one, committing the offset only after the
// event was recorded (or dead-lettered): at-least-once delivery.
type Consumer struct {
	source     Source
	dlq        Sink
	applier    Applier
	log        *slog.Logger
	maxRetries int
	baseDelay  time.Duration
	onResult   func(result string)
}

// Options configures a Consumer.
type Options struct {
	MaxRetries int
	BaseDelay  time.Duration
	// OnResult is called once per message with the outcome (recorded,
	// duplicate, dead_letter). Used to feed Prometheus counters.
	OnResult func(result string)
}

// New builds a Consumer.
func New(src Source, dlq Sink, app Applier, log *slog.Logger, opts Options) *Consumer {
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 5
	}
	if opts.BaseDelay <= 0 {
		opts.BaseDelay = 200 * time.Millisecond
	}
	if opts.OnResult == nil {
		opts.OnResult = func(string) {}
	}
	return &Consumer{source: src, dlq: dlq, applier: app, log: log, maxRetries: opts.MaxRetries, baseDelay: opts.BaseDelay, onResult: opts.OnResult}
}

// Run blocks until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.source.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}
		if err := c.handle(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := c.source.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var ev ledger.PaymentEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return c.deadLetter(ctx, msg, fmt.Errorf("decode: %w", err))
	}
	for attempt := 0; ; attempt++ {
		res, err := c.applier.Apply(ctx, ev)
		switch {
		case err == nil:
			c.onResult(string(res))
			c.log.InfoContext(ctx, "event processed", "event_id", ev.EventID, "result", res, "partition", msg.Partition, "offset", msg.Offset)
			return nil
		case errors.Is(err, ledger.ErrInvalidEvent):
			// Business validation errors never succeed on retry.
			return c.deadLetter(ctx, msg, err)
		case attempt >= c.maxRetries:
			return c.deadLetter(ctx, msg, fmt.Errorf("after %d retries: %w", attempt, err))
		}
		delay := c.baseDelay * time.Duration(1<<attempt)
		c.log.WarnContext(ctx, "retrying event", "event_id", ev.EventID, "attempt", attempt+1, "delay", delay, "error", err)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Consumer) deadLetter(ctx context.Context, msg kafka.Message, cause error) error {
	c.onResult("dead_letter")
	c.log.ErrorContext(ctx, "sending event to dead-letter topic", "offset", msg.Offset, "error", cause)
	dl := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: append(msg.Headers,
			kafka.Header{Key: "x-error", Value: []byte(cause.Error())},
			kafka.Header{Key: "x-original-topic", Value: []byte(msg.Topic)},
		),
	}
	if err := c.dlq.WriteMessages(ctx, dl); err != nil {
		return fmt.Errorf("write dead-letter: %w", err)
	}
	return nil
}
