package butterfly

import (
	"github.com/cactus/go-statsd-client/v5/statsd"
	"time"
)

type StatsdConfig struct {
	Endpoint string
	PREFIX   string
}

type StatsdClient struct {
	client statsd.Statter
}

func NewStatsdClient(config StatsdConfig) (*StatsdClient, error) {
	var clientConfig = &statsd.ClientConfig{
		Address: config.Endpoint,
		Prefix:  config.PREFIX,
	}
	statsDClient, err := statsd.NewClientWithConfig(clientConfig)

	if err != nil {
		return nil, err
	}

	var client = &StatsdClient{client: statsDClient}

	return client, err
}

func (m *StatsdClient) dispose() {
	if m.client != nil {
		m.client.Close()
	}
}

func getCurrentTimeMilliseconds(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func getTimeDelta(t time.Time) int64 {
	return getCurrentTimeMilliseconds(time.Now()) - getCurrentTimeMilliseconds(t)
}

func (m *StatsdClient) PublishKafkaReadOps(topic string) {
	m.client.Timing("kafka.read."+topic+".ops", 1, 1)
}

func (m *StatsdClient) PublishKafkaReadError() {
	m.client.Timing("error.kafka.read", 1, 1)
}

func (m *StatsdClient) PublishKafkaWriteLatency(topic string, startTime time.Time) {
	m.client.Timing("kafka."+topic+".write.latency", getTimeDelta(startTime), 1)
}

func (m *StatsdClient) PublishKafkaWriteOps(topic string) {
	m.client.Timing("kafka."+topic+".write.ops", 1, 1)
}

func (m *StatsdClient) PublishKafkaWriteError(topic string) {
	m.client.Timing("error.kafka.write."+topic, 1, 1)
}
