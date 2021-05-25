package butterfly

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaWriter struct {
	writer *kafka.Writer
	topic  string
	logger Logger
	errorLogger Logger
	statsdClient *StatsdClient
}

func NewKafkaWriter(writerConfig *WriterConfig) *KafkaWriter {
	w := &KafkaWriter{
		topic: writerConfig.Topic,
		logger: writerConfig.Logger,
		errorLogger: writerConfig.ErrorLogger,
		statsdClient: writerConfig.StatsdClient,
	}

	w.writer = &kafka.Writer{
		Addr:         kafka.TCP(writerConfig.Endpoint...),
		Topic:        writerConfig.Topic,
		BatchTimeout: writerConfig.BatchTimeout,
		WriteTimeout: writerConfig.WriteTimeout,
		BatchSize: writerConfig.BatchSize,
	}
	return w
}

func (w *KafkaWriter) Write(ctx context.Context, logs []interface{}) error {
	startTime := time.Now()
	w.statsdClient.PublishKafkaWriteOps(w.topic)
	defer func() {
		w.statsdClient.PublishKafkaWriteLatency(w.topic, startTime)
	}()

	var messages []kafka.Message
	for _, log := range logs {
		w.logger.Printf("[butterfly] Topic:: %s -- message: %+v", w.topic, log)
		val, err := json.Marshal(log)
		if err != nil {
			w.logger.Printf("[butterfly] Error in Marshalling %s", err.Error())
			return err
		}
		messages = append(messages, kafka.Message{Value: val})
	}

	err := w.writer.WriteMessages(ctx, messages...)
	if err != nil {
		w.statsdClient.PublishKafkaWriteError(w.topic)
		w.logger.Printf("[butterfly] Error in Writing.. %s", err.Error())
	}
	return err
}

func (w *KafkaWriter) Dispose() error {
	return w.writer.Close()
}
