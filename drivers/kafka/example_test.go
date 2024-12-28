package kafka_test

import (
	"context"
	"fmt"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/thatcatdev/ep/drivers/kafka"
	"math/rand"
	"time"
)

func randomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func ExampleKafkaDriver() {
	consumerGroupName := randomString(10)
	brokerHost := "localhost:9092"
	topicName := randomString(10)
	config := kafka.KafkaConfig{
		ConsumerGroupName:        consumerGroupName,
		BootstrapServers:         brokerHost,
		SaslMechanism:            nil,
		SecurityProtocol:         nil,
		Username:                 nil,
		Password:                 nil,
		ConsumerSessionTimeoutMs: nil,
		ConsumerAutoOffsetReset:  nil,
		ClientID:                 nil,
		Debug:                    nil,
	}
	driver := kafka.NewKafkaDriver(&config)
	defer driver.Close()

	messageReceived := make(chan string)
	// create topic
	err := driver.CreateTopic(context.Background(), topicName)
	if err != nil {
		fmt.Println(err)
	}

	go func() {
		_ = driver.Consume(nil, topicName, func(ctx context.Context, originalMessage *kafka2.Message, message []byte) error {
			messageReceived <- string(message)
			return nil
		})
	}()

	time.Sleep(5 * time.Second)

	err = driver.Produce(context.Background(), topicName, &kafka2.Message{
		Value: []byte("test"),
	})

	// wait for message to be consumed
	select {
	case msg := <-messageReceived:
		fmt.Println(msg)
	case <-time.After(5 * time.Second):
		fmt.Println("timeout")
	}

}
