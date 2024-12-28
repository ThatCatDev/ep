package kafka

import (
	"context"
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/thatcatdev/ep/drivers"
	"github.com/thatcatdev/ep/event"
)

type KafkaDriver struct {
	consumer    *kafka.Consumer
	producer    *kafka.Producer
	kafkaConfig *KafkaConfig
	config      *kafka.ConfigMap
	client      *kafka.AdminClient
}

func NewKafkaDriver(config *KafkaConfig) drivers.Driver[*kafka.Message] {
	cfg := GetKafkaConfig(*config)
	admin, err := kafka.NewAdminClient(cfg)
	if err != nil {
		panic(err)
	}

	return &KafkaDriver{
		client:      admin,
		config:      cfg,
		kafkaConfig: config,
	}
}

func (k *KafkaDriver) Consume(ctx context.Context, topic string, handler func(context.Context, *kafka.Message, []byte) error) error {
	cfg := GetKafkaConsumerConfig(*k.kafkaConfig)
	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return err
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		return err
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			return err
		}

		if err := handler(ctx, msg, msg.Value); err != nil {
			return err
		}
	}
}

func (k *KafkaDriver) Produce(ctx context.Context, topic string, message []byte) error {
	producer, err := kafka.NewProducer(k.config)
	if err != nil {
		return err
	}

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, deliveryChan)
	if err != nil {
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}

	return nil
}

func (k *KafkaDriver) CreateTopic(ctx context.Context, topic string) error {
	_, err := k.client.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}})
	if err != nil {
		return err
	}

	return nil
}

func (k *KafkaDriver) Close() error {
	if k.consumer != nil {
		k.consumer.Close()
	}
	if k.producer != nil {
		k.producer.Close()
	}

	k.client.Close()

	return nil
}

func (k *KafkaDriver) ExtractEvent(data *kafka.Message) (*event.SubData[*kafka.Message], error) {
	eventData := &event.SubData[*kafka.Message]{
		DriverMessage: data,
	}
	headers := map[string]string{}
	for _, v := range data.Headers {
		headers[v.Key] = string(v.Value)
	}

	eventData.Headers = headers

	msgByte, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(msgByte, &eventData.RawData)

	return eventData, err
}
