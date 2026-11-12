package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"sentinel/internal/event"
)

type KafkaPublisher struct {
	w *kafka.Writer
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{w: &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		// murmur2 как в java-клиенте, ключ -> партиция одинаково на любом языке
		Balancer:     &kafka.Murmur2Balancer{},
		RequiredAcks: kafka.RequireAll,
		// дефолт 1с, при синхронной записи это +1с к каждому запросу
		BatchTimeout:           5 * time.Millisecond,
		BatchSize:              1000,
		Compression:            kafka.Snappy,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: false,
	}}
}

func (p *KafkaPublisher) Publish(ctx context.Context, events []event.Event) error {
	msgs, err := EncodeMessages(events)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, msgs...)
}

func EncodeMessages(events []event.Event) ([]kafka.Message, error) {
	msgs := make([]kafka.Message, len(events))
	for i, ev := range events {
		b, err := json.Marshal(ev)
		if err != nil {
			return nil, fmt.Errorf("marshal event %s: %w", ev.ID, err)
		}
		msgs[i] = kafka.Message{
			// ключ = ip: события одного ip попадают в одну партицию и читаются по порядку
			Key:   []byte(ev.SourceIP),
			Value: b,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(ev.Type)},
			},
		}
	}
	return msgs, nil
}

func (p *KafkaPublisher) Close() error { return p.w.Close() }
