package butterfly

import (
	"context"
	"time"
)

const (
	LastOffset  int64 = -1 // The most recent offset available for a partition.
	FirstOffset int64 = -2 // The least recent offset available for a partition.
)

type ReadMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time
}

type WriteMessage struct {
	Key   []byte
	Value []byte
	Topic string
}

type Writer interface {
	Write(ctx context.Context, logs ...WriteMessage) error
}

type Reader interface {
	ReadMessage(ctx context.Context) (ReadMessage, error)
	FetchMessage(ctx context.Context) (ReadMessage, error)
}

type WriterConfig struct {
	Endpoint     []string
	Topic        string
	BatchTimeout time.Duration
	WriteTimeout time.Duration
	BatchSize    int
	Logger       Logger
	ErrorLogger  Logger
	StatsdClient *StatsdClient
}

type ReaderConfig struct {
	Endpoint       []string
	GroupId        string
	Topics         []string
	Offset         int64
	QueueCapacity  int
	CommitInterval time.Duration
	Logger         Logger
	ErrorLogger    Logger
	StatsdClient   *StatsdClient
}

func NewWriter(writerConfig *WriterConfig) Writer {

	writer := NewKafkaWriter(writerConfig)
	return writer
}

func NewReader(readerConfig *ReaderConfig) Reader {

	reader := NewKafkaReader(readerConfig)
	return reader
}
