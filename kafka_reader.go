package butterfly

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type KafkaReader struct {
	reader  *kafka.Reader
	groupId string
	topics   []string
	logger Logger
	errorLogger Logger
	statsdClient StatsdClient
}

func NewKafkaReader(config *ReaderConfig) *KafkaReader {
	
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       config.Endpoint,
		GroupID:       config.GroupId,
		GroupTopics:   config.Topics,
		StartOffset:   config.Offset,
		QueueCapacity: config.QueueCapacity,
		//CommitInterval: 5 * time.Second,
	})

	r := &KafkaReader{
		reader:  reader,
		groupId: config.GroupId,
		topics:   config.Topics,
	}
	return r
}

/*
	Also commits before returning
 */
func (r *KafkaReader) Read(ctx context.Context) (Message, error) {
	r.logger.Printf("[butterfly] Fetching message : %v", r.groupId)

	kMessage, err := r.reader.ReadMessage(ctx)
	r.statsdClient.PublishKafkaReadOps(kMessage.Topic)

	if err != nil {
		r.errorLogger.Printf("[butterfly] Error in fetching message %v -- %s", r.groupId, err.Error())
		r.statsdClient.PublishKafkaReadError()
		return Message{}, err
	}
	r.logger.Printf("[butterfly] Fetched message %v:%v :: %+v", kMessage.Topic, r.groupId, kMessage)

	return Message{
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
func (r KafkaReader) FetchMessage(ctx context.Context) (Message, error) {
	r.logger.Printf("[butterfly] Fetching message : %v", r.groupId)

	kMessage, err := r.reader.FetchMessage(ctx)
	r.statsdClient.PublishKafkaReadOps(kMessage.Topic)

	if err != nil {
		r.errorLogger.Printf("[butterfly] Error in fetching message %v -- %s", r.groupId, err.Error())
		r.statsdClient.PublishKafkaReadError()
		return Message{}, err
	}
	r.logger.Printf("[butterfly] Fetched message %v:%v :: %+v", kMessage.Topic, r.groupId, kMessage)

	return Message{
		Topic:     kMessage.Topic,
		Partition: kMessage.Partition,
		Offset:    kMessage.Offset,
		Key:       kMessage.Key,
		Value:     kMessage.Value,
		Time:      kMessage.Time,
	}, err
}
