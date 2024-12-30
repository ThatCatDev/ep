package processor_test

import (
	"context"
	"encoding/json"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/thatcatdev/ep/drivers/kafka"
	"github.com/thatcatdev/ep/event"
	"github.com/thatcatdev/ep/middleware"
	"github.com/thatcatdev/ep/processor"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func RandomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestNewProcessor(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		a := assert.New(t)
		consumerGroupName := RandomString(10)
		topicName := RandomString(10)
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
		a.NotNil(driver)

		err := driver.CreateTopic(context.Background(), topicName)
		a.Nil(err)
		time.Sleep(2 * time.Second)

		type Payload struct {
			Count int
		}

		// mutex counter
		type Counter struct {
			sync.Mutex
			Count int
		}

		counter := &Counter{
			Count: 0,
		}

		processorInstance := processor.NewProcessor[*kafka2.Message, Payload](driver, topicName, func(ctx context.Context, data event.Event[*kafka2.Message, Payload]) (event.Event[*kafka2.Message, Payload], error) {
			counter.Lock()
			counter.Count++
			counter.Unlock()
			return data, nil
		})

		a.NotNil(processorInstance)

		go func() {
			_ = processorInstance.Run(context.Background())
		}()

		time.Sleep(5 * time.Second)

		payload := Payload{Count: 0}

		payloadBytes, _ := json.Marshal(payload)
		// produce message
		producer := kafka.NewKafkaDriver(&config)
		_ = producer.Produce(context.Background(), topicName, &kafka2.Message{
			Value: payloadBytes,
		})
		_ = producer.Close()

		time.Sleep(5 * time.Second)
		counter.Lock()
		defer counter.Unlock()
		a.Equal(1, counter.Count)

	})

	t.Run("Processor with middleware", func(t *testing.T) {
		a := assert.New(t)
		consumerGroupName := RandomString(10)
		topicName := RandomString(10)
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
		a.NotNil(driver)

		defer func() {
			err := driver.Close()
			a.Nil(err)
		}()

		err := driver.CreateTopic(context.Background(), topicName)
		a.Nil(err)
		time.Sleep(2 * time.Second)

		type Payload struct {
			Count int
		}

		// mutex counter
		type Counter struct {
			sync.Mutex
			Count int
		}

		counter := &Counter{
			Count: 0,
		}

		processorInstance := processor.NewProcessor[*kafka2.Message, Payload](driver, topicName, func(ctx context.Context, data event.Event[*kafka2.Message, Payload]) (event.Event[*kafka2.Message, Payload], error) {
			counter.Lock()
			counter.Count++
			counter.Unlock()
			return data, nil
		})

		a.NotNil(processorInstance)

		middlewareFunc := func(ctx context.Context, data event.Event[*kafka2.Message, Payload], next middleware.Handler[*kafka2.Message, Payload]) (*event.Event[*kafka2.Message, Payload], error) {
			data.Payload.Count++
			counter.Lock()
			counter.Count++
			counter.Unlock()
			return next(ctx, data)
		}

		go func() {
			err := processorInstance.
				AddMiddleware(middlewareFunc).
				Run(context.Background())
			if err != nil {
				panic(err)
			}
		}()

		time.Sleep(1 * time.Second)

		payload := Payload{Count: 0}

		payloadBytes, _ := json.Marshal(payload)
		// produce message
		producer := kafka.NewKafkaDriver(&config)
		_ = producer.Produce(context.Background(), topicName, &kafka2.Message{
			Value: payloadBytes,
		})

		time.Sleep(5 * time.Second)
		counter.Lock()
		defer counter.Unlock()
		a.Equal(2, counter.Count)
	})
	t.Run("Processor with middleware and short circuit", func(t *testing.T) {
		a := assert.New(t)
		consumerGroupName := RandomString(10)
		topicName := RandomString(10)
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
		a.NotNil(driver)
		defer func() {
			err := driver.Close()
			a.Nil(err)
		}()

		err := driver.CreateTopic(context.Background(), topicName)
		a.Nil(err)

		defer func() {
			err := driver.Close()
			a.Nil(err)
		}()

		time.Sleep(2 * time.Second)

		type Payload struct {
			Count int
		}

		// mutex counter
		type Counter struct {
			sync.Mutex
			Count int
		}

		counter := &Counter{
			Count: 0,
		}

		processorInstance := processor.NewProcessor[*kafka2.Message, Payload](driver, topicName, func(ctx context.Context, data event.Event[*kafka2.Message, Payload]) (event.Event[*kafka2.Message, Payload], error) {
			counter.Lock()
			counter.Count++
			counter.Unlock()
			return data, nil
		})

		a.NotNil(processorInstance)

		middlewareFunc := func(ctx context.Context, data event.Event[*kafka2.Message, Payload], next middleware.Handler[*kafka2.Message, Payload]) (*event.Event[*kafka2.Message, Payload], error) {
			data.Payload.Count++
			counter.Lock()
			counter.Count++
			counter.Unlock()
			return &data, nil
		}
		middlewareFuncFail := func(ctx context.Context, data event.Event[*kafka2.Message, Payload], next middleware.Handler[*kafka2.Message, Payload]) (*event.Event[*kafka2.Message, Payload], error) {

			return &data, assert.AnError
		}

		go func() {
			_ = processorInstance.
				AddMiddleware(middlewareFunc).
				AddMiddleware(middlewareFuncFail).
				AddMiddleware(middlewareFunc).
				Run(context.Background())
		}()

		time.Sleep(1 * time.Second)

		payload := Payload{Count: 0}

		payloadBytes, _ := json.Marshal(payload)
		// produce message
		producer := kafka.NewKafkaDriver(&config)
		_ = producer.Produce(context.Background(), topicName, &kafka2.Message{
			Value: payloadBytes,
		})

		time.Sleep(5 * time.Second)
		counter.Lock()
		defer counter.Unlock()
		a.Equal(1, counter.Count)
	})
}
