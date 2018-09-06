package kafka-asmara

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

var kafkaVersion = map[string]sarama.KafkaVersion{
	"V0_10_2_0": sarama.V0_10_2_0,
	"V0_11_0_2": sarama.V0_11_0_2,
}

type Consumer interface {
	GetConsumer() *cluster.Consumer
}

type kafkaConsumer struct {
	Consumer *cluster.Consumer
}

func getConsumerConfig(version string) *cluster.Config {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Mode = cluster.ConsumerModePartitions
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	if _, ok := kafkaVersion[version]; ok {
		config.Version = kafkaVersion[version]
	}
	return config
}

func newSaramaConsumer(address, topics []string, groupId, version string) (*cluster.Consumer, error) {
	consumer, err := cluster.NewConsumer(address, groupId, topics, getConsumerConfig(version))
	return consumer, err
}

func NewConsumer(address, topics []string, groupId, version string) Consumer {
	consumer, err := newSaramaConsumer(address, topics, groupId, version)
	if err != nil {
		return nil
	}
	return &kafkaConsumer{
		Consumer: consumer,
	}

}

func (c *kafkaConsumer) GetConsumer() *cluster.Consumer {
	return c.Consumer
}

