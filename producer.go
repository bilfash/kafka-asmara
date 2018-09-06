package kafka-asmara

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type Producer struct {
	syncProducer sarama.SyncProducer
}

func NewProducer(brokerAddress []string, maxRetry int, returnSuccess bool) Producer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = returnSuccess

	syncProducer, err := sarama.NewSyncProducer(brokerAddress, config)
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}

	return Producer{
		syncProducer: syncProducer,
	}
}

func (p Producer) SendMessage(topic string, data []byte) (partition int32, offset int64, err error) {
	var saramaByte sarama.ByteEncoder
	saramaByte = data

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: saramaByte,
	}

	partition, offset, err = p.syncProducer.SendMessage(msg)
	return
}

func (p Producer) Close() error {
	return p.syncProducer.Close()
}

