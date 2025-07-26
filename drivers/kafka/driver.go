package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ThatCatDev/ep/drivers"
	"github.com/ThatCatDev/ep/event"
)

type KafkaDriver struct {
	producer    *kafka.Producer
	kafkaConfig *KafkaConfig
	config      *kafka.ConfigMap
	client      *kafka.AdminClient
	mu          sync.Mutex
}

func NewKafkaDriver(config *KafkaConfig) drivers.Driver[*kafka.Message] {
	cfg := GetKafkaConfig(*config)
	admin, err := kafka.NewAdminClient(cfg)
	if err != nil {
		panic(err)
	}
	if admin == nil {
		panic("admin client is nil")
	}

	return &KafkaDriver{
		client:      admin,
		config:      cfg,
		kafkaConfig: config,
	}
}

func (k *KafkaDriver) Consume(ctx context.Context, topic string, handler func(context.Context, *kafka.Message, []byte) error) error {
	cfg := GetKafkaConsumerConfig(*k.kafkaConfig)
	//nolint:errcheck
	_ = cfg.SetKey("enable.auto.commit", false)

	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	if err := consumer.Subscribe(topic, nil); err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := consumer.ReadMessage(time.Second)
			if err != nil {
				kafkaErr, ok := err.(kafka.Error)
				if ok && (kafkaErr.IsRetriable() || kafkaErr.Code() == kafka.ErrTimedOut) {
					continue // not a real error
				}

				return fmt.Errorf("read error: %w", err)
			}
			if msg == nil || msg.Value == nil {
				continue
			}
			if err := handler(ctx, msg, msg.Value); err != nil {
				return err
			}
			if _, err := consumer.CommitMessage(msg); err != nil {
				return fmt.Errorf("commit error: %w", err)
			}
		}
	}
}

func (k *KafkaDriver) Produce(ctx context.Context, topic string, message *kafka.Message) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.producer == nil {
		producer, err := kafka.NewProducer(k.config)
		if err != nil {
			return fmt.Errorf("failed to create producer: %w", err)
		}
		k.producer = producer
	}

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message.Value,
		Headers:        message.Headers,
		Key:            message.Key,
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

	return err
}

func (k *KafkaDriver) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	var firstErr error

	if k.producer != nil {
		k.producer.Close()
		k.producer = nil
	}

	if k.client != nil {
		k.client.Close()
		k.client = nil
	}

	return firstErr
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
