package detector

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"sentinel/internal/event"
)

var (
	consumedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sentinel_detector_events_total",
		Help: "Events consumed, by result (processed, duplicate, dead_lettered).",
	}, []string{"result"})
	retriesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sentinel_detector_retries_total",
		Help: "Processing attempts that failed with a transient error and were retried.",
	})
	processingSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sentinel_detector_processing_seconds",
		Help:    "Time to run all rules for one event.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	})
	consumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sentinel_detector_consumer_lag",
		Help: "Messages behind the end of the assigned partitions, as seen by this reader.",
	})
)

type Handler interface {
	Handle(ctx context.Context, ev event.Event) (duplicate bool, err error)
}

type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Consumer struct {
	reader  MessageReader
	dlq     MessageWriter
	handler Handler
	log     *slog.Logger
	backoff Backoff
}

func NewConsumer(r MessageReader, dlq MessageWriter, h Handler, log *slog.Logger) *Consumer {
	return &Consumer{reader: r, dlq: dlq, handler: h, log: log, backoff: DefaultBackoff}
}

// at-least-once: оффсет коммитим только после обработки.
// битое сообщение -> в DLQ и идём дальше, ошибка redis/postgres -> ретраим и не коммитим
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		ev, decodeErr := event.Decode(msg.Value)
		if decodeErr != nil {
			c.log.WarnContext(ctx, "dead-lettering invalid message",
				"partition", msg.Partition, "offset", msg.Offset, "err", decodeErr)
			if err := c.retry(ctx, func() error { return c.deadLetter(ctx, msg, decodeErr) }); err != nil {
				return nil
			}
			consumedTotal.WithLabelValues("dead_lettered").Inc()
		} else {
			var dup bool
			start := time.Now()
			if err := c.retry(ctx, func() (err error) {
				dup, err = c.handler.Handle(ctx, ev)
				return err
			}); err != nil {
				return nil
			}
			processingSeconds.Observe(time.Since(start).Seconds())
			if dup {
				consumedTotal.WithLabelValues("duplicate").Inc()
			} else {
				consumedTotal.WithLabelValues("processed").Inc()
			}
		}

		if err := c.retry(ctx, func() error { return c.reader.CommitMessages(ctx, msg) }); err != nil {
			return nil
		}
	}
}

func (c *Consumer) retry(ctx context.Context, fn func() error) error {
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		retriesTotal.Inc()
		wait := c.backoff.Delay(attempt)
		c.log.ErrorContext(ctx, "transient error, will retry", "err", err, "attempt", attempt+1, "wait", wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (c *Consumer) deadLetter(ctx context.Context, msg kafka.Message, cause error) error {
	headers := append([]kafka.Header{}, msg.Headers...)
	headers = append(headers,
		kafka.Header{Key: "dlq_error", Value: []byte(cause.Error())},
		kafka.Header{Key: "dlq_original_topic", Value: []byte(msg.Topic)},
		kafka.Header{Key: "dlq_original_partition", Value: []byte(strconv.Itoa(msg.Partition))},
		kafka.Header{Key: "dlq_original_offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
	)
	return c.dlq.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: msg.Value, Headers: headers})
}

func ReportLag(ctx context.Context, r *kafka.Reader, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			consumerLag.Set(float64(r.Stats().Lag))
		}
	}
}

type Backoff struct {
	Base, Max time.Duration
}

var DefaultBackoff = Backoff{Base: 100 * time.Millisecond, Max: 10 * time.Second}

func (b Backoff) Delay(attempt int) time.Duration {
	if attempt > 20 {
		return b.Max
	}
	d := b.Base << attempt
	if d <= 0 || d > b.Max {
		return b.Max
	}
	return d
}
