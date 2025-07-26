package processor_test

import (
	"context"
	"github.com/ThatCatDev/ep/v2/drivers/kafka"
	"github.com/ThatCatDev/ep/v2/event"
	"github.com/ThatCatDev/ep/v2/processor"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"math/rand"
)

func randomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

type payload struct {
	Count int
}

func ExampleProcessor() {
	ctx := context.Background()
	consumerGroupName := randomString(10)
	topicName := randomString(10)
	brokerHost := "localhost:9092"
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

	_ = driver.CreateTopic(ctx, topicName)

	messageReceived := make(chan *kafka2.Message)

	processorInstance := processor.NewProcessor[*kafka2.Message, payload](driver, topicName, func(ctx context.Context, data event.Event[*kafka2.Message, payload]) (event.Event[*kafka2.Message, payload], error) {
		log.Println("Processing data")
		log.Printf("Data: %+v", data)
		messageReceived <- data.DriverMessage
		return data, nil
	})

	go func() {
		err := processorInstance.Run(ctx)
		if err != nil {
			log.Fatalf("failed to run processor: %v", err)
		}
	}()

	// Produce a message
	err := driver.Produce(ctx, topicName, &kafka2.Message{
		Value: []byte("{\"Count\": 1}"),
	})
	if err != nil {
		log.Fatalf("failed to produce message: %v", err)
	}

	// Wait for the message to be processed
	msg := <-messageReceived
	log.Printf("Message received: %+v", msg)

}
