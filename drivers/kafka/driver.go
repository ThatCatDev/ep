package kafka

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type KafkaDriver struct {
	client *kafka.Producer
}
