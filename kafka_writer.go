package butterfly

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type KafkaWriter struct {
	writer       *kafka.Writer
	topic        string
}

func NewKafkaWriter(writerConfig *WriterConfig) *KafkaWriter {
	w := &KafkaWriter{
		topic:        writerConfig.Topic,
	}

	w.writer = &kafka.Writer{
		Addr:         kafka.TCP(writerConfig.Endpoint...),
		Topic:        writerConfig.Topic,
		BatchTimeout: writerConfig.BatchTimeout,
		WriteTimeout: writerConfig.WriteTimeout,
		BatchSize:    writerConfig.BatchSize,
	}
	return w
}

func (w *KafkaWriter) Write(ctx context.Context, logs ...WriteMessage) error {
	var messages []kafka.Message
	for _, log := range logs {
		var topic = log.Topic
		if len(topic) == 0 {
			topic = w.topic
		}
		if len(topic) == 0 {
			continue // No topic, no use of writing
		}

		messages = append(messages, kafka.Message{
			Value: log.Value,
			Key:   log.Key,
			Topic: topic,
		})
	}

	err := w.writer.WriteMessages(ctx, messages...)
	return err
}

func (w *KafkaWriter) Dispose() error {
	return w.writer.Close()
}
