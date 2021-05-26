package butterfly

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type KafkaReader struct {
	reader       *kafka.Reader
	groupId      string
	topics       []string
}

func NewKafkaReader(config *ReaderConfig) *KafkaReader {

	var startOffset = config.StartOffset
	if config.StartOffset == 0 {
		startOffset = LastOffset
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       config.Endpoint,
		GroupID:       config.GroupId,
		GroupTopics:   config.Topics,
		StartOffset:   startOffset,
		QueueCapacity: config.QueueCapacity,
		//CommitInterval: 5 * time.Second,
	})

	r := &KafkaReader{
		reader:  reader,
		groupId: config.GroupId,
		topics:  config.Topics,
	}
	return r
}

/*
	Also commits before returning
*/
func (r *KafkaReader) ReadMessage(ctx context.Context) (ReadMessage, error) {
	kMessage, err := r.reader.ReadMessage(ctx)

	if err != nil {
		return ReadMessage{}, err
	}

	return ReadMessage{
		Topic:     kMessage.Topic,
		Partition: kMessage.Partition,
		Offset:    kMessage.Offset,
		Key:       kMessage.Key,
		Value:     kMessage.Value,
		Time:      kMessage.Time,
	}, err
}

/*
	FetchMessage does not commit the message
	Invoke CommitMessage to commit the messages
*/
func (r *KafkaReader) FetchMessage(ctx context.Context) (ReadMessage, error) {

	kMessage, err := r.reader.FetchMessage(ctx)

	if err != nil {
		return ReadMessage{}, err
	}

	return ReadMessage{
		Topic:     kMessage.Topic,
		Partition: kMessage.Partition,
		Offset:    kMessage.Offset,
		Key:       kMessage.Key,
		Value:     kMessage.Value,
		Time:      kMessage.Time,
	}, err
}
