package backoffretry_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	kafka2 "github.com/ThatCatDev/ep/drivers/kafka"
	"github.com/ThatCatDev/ep/drivers/mocks"
	"github.com/ThatCatDev/ep/event"
	"github.com/ThatCatDev/ep/middlewares/kafka/backoffretry"
	"github.com/ThatCatDev/ep/processor"
	"go.uber.org/mock/gomock"
	"math/rand"
	"sync"
	"testing"
	"time"
)

type payload struct {
	Count int
}

func RandomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestNewBackoffRetry(t *testing.T) {
	t.Run("TestNewBackoffRetry", func(t *testing.T) {

		ctrl := gomock.NewController(t)

		driver := mocks.NewMockDriver[*kafka.Message](ctrl)

		backoffretryInstance := backoffretry.NewBackoffRetry[payload](driver, backoffretry.Config{
			MaxRetries: 3,
			HeaderKey:  "retry",
			RetryQueue: "retry-queue",
		})

		if backoffretryInstance == nil {
			t.Fatalf("failed to create backoffretry")
		}
	})

	t.Run("TestNewBackoffRetryWithNilDriver", func(t *testing.T) {
		a := assert.New(t)
		ctrl := gomock.NewController(t)

		driver := mocks.NewMockDriver[*kafka.Message](ctrl)

		driver.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		backoffretryInstance := backoffretry.NewBackoffRetry[payload](driver, backoffretry.Config{
			MaxRetries: 3,
			HeaderKey:  "retry",
			RetryQueue: "retry-queue",
		})

		message := &kafka.Message{
			Headers: []kafka.Header{},
			Value:   []byte("{\"Count\":0}"),
		}

		messageEvent := &event.Event[*kafka.Message, payload]{
			Headers:       map[string]string{},
			DriverMessage: message,
			RawData:       map[string]interface{}{},
			Payload:       payload{},
		}

		messageEvent, err := backoffretryInstance.Process(context.Background(), *messageEvent, func(ctx context.Context, data event.Event[*kafka.Message, payload]) (*event.Event[*kafka.Message, payload], error) {
			return &data, fmt.Errorf("error")
		})

		a.Nil(err)

		a.Equal("1", messageEvent.Headers["retry"])

		messageEvent, err = backoffretryInstance.Process(context.Background(), *messageEvent, func(ctx context.Context, data event.Event[*kafka.Message, payload]) (*event.Event[*kafka.Message, payload], error) {
			return &data, fmt.Errorf("error")
		})

		a.Nil(err)

		a.Equal("2", messageEvent.Headers["retry"])

		messageEvent, err = backoffretryInstance.Process(context.Background(), *messageEvent, func(ctx context.Context, data event.Event[*kafka.Message, payload]) (*event.Event[*kafka.Message, payload], error) {
			return &data, fmt.Errorf("error")
		})

		a.Nil(err)

		a.Equal("3", messageEvent.Headers["retry"])

	})
}

func TestProcessorWithBackoffRetry(t *testing.T) {
	t.Run("Processor with backoff retry", func(t *testing.T) {
		a := assert.New(t)
		ctx := context.Background()
		consumerGroupName := RandomString(10)
		topicName := RandomString(10)
		brokerHost := "localhost:9092"
		config := kafka2.KafkaConfig{
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
		driver := kafka2.NewKafkaDriver(&config)
		a.NotNil(driver)

		defer func() {
			err := driver.Close()
			a.Nil(err)
		}()

		err := driver.CreateTopic(context.Background(), topicName)
		a.Nil(err)

		time.Sleep(5 * time.Second)

		backoffretryInstance := backoffretry.NewBackoffRetry[payload](driver, backoffretry.Config{
			MaxRetries: 3,
			HeaderKey:  "retry",
			RetryQueue: topicName,
		})

		type Counter struct {
			sync.Mutex
			Count int
		}

		counter := &Counter{
			Count: 0,
		}

		processorInstance := processor.NewProcessor[*kafka.Message, payload](driver, topicName, func(ctx context.Context, data event.Event[*kafka.Message, payload]) (event.Event[*kafka.Message, payload], error) {
			t.Logf("message received: %v", data)
			counter.Lock()
			counter.Count++
			counter.Unlock()
			return data, fmt.Errorf("error")
		})

		go func() {
			err := processorInstance.
				AddMiddleware(backoffretryInstance.Process).
				Run(ctx)
			if err != nil && ctx.Err() == nil { // Ignore error if caused by context cancellation
				t.Errorf("error consuming messages: %v", err)
			}
		}()
		time.Sleep(5 * time.Second)

		payload := payload{Count: 0}

		payloadBytes, _ := json.Marshal(payload)

		_ = driver.Produce(context.Background(), topicName, &kafka.Message{
			Value: payloadBytes,
		})

		time.Sleep(15 * time.Second)
		counter.Lock()
		defer counter.Unlock()
		a.Equal(3, counter.Count)

	})
}
