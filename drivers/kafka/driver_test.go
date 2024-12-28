package kafka_test

import (
	"context"
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/thatcatdev/ep/drivers/kafka"
	"math/rand"
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

func TestNewKafkaDriver(t *testing.T) {
	t.Run("TestNewKafkaDriver", func(t *testing.T) {
		t.Parallel()
		a := assert.New(t)
		consumerGroupName := RandomString(10)
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

		err := driver.Close()
		a.Nil(err)
	})

	t.Run("TestNewKafkaDriverWithInvalidConfig", func(t *testing.T) {
		t.Parallel()
		a := assert.New(t)
		// catch with recover
		defer func() {
			if r := recover(); r != nil {
				a.NotNil(r)
			}
		}()
		driver := kafka.NewKafkaDriver(nil)
		a.Nil(driver)

	})

	t.Run("TestConsume", func(t *testing.T) {
		t.Parallel()
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

		messageReceived := make(chan string)
		// create topic
		err := driver.CreateTopic(context.Background(), topicName)
		a.Nil(err)
		go func() {
			_ = driver.Consume(nil, topicName, func(ctx context.Context, originalMessage *kafka2.Message, message []byte) error {
				messageReceived <- string(message)
				return nil
			})
		}()

		time.Sleep(5 * time.Second)

		err = driver.Produce(context.Background(), topicName, []byte("test"))

		// wait for message to be consumed
		select {
		case msg := <-messageReceived:
			t.Logf("test passed: received message '%s'", msg)
		case <-time.After(5 * time.Second):
			t.Errorf("message not consumed")
		}

		err = driver.Close()
		a.Nil(err)
	})

	t.Run("TestConsumeManyMessages", func(t *testing.T) {
		t.Parallel()
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

		messageReceived := make(chan string)
		// create topic
		err := driver.CreateTopic(context.Background(), topicName)
		a.Nil(err)
		go func() {
			_ = driver.Consume(nil, topicName, func(ctx context.Context, originalMessage *kafka2.Message, message []byte) error {
				messageReceived <- string(message)
				return nil
			})
		}()

		time.Sleep(5 * time.Second)

		for i := 0; i < 10; i++ {
			err = driver.Produce(context.Background(), topicName, []byte("test"))
			a.Nil(err)
		}

		// wait for message to be consumed
		for i := 0; i < 10; i++ {
			select {
			case msg := <-messageReceived:
				t.Logf("test passed: received message '%s'", msg)
			case <-time.After(5 * time.Second):
				t.Errorf("message not consumed")
			}
		}

		err = driver.Close()
		a.Nil(err)
	})
}

func TestKafkaDriver_ExtractEvent(t *testing.T) {
	t.Run("TestKafkaDriver_ExtractEvent", func(t *testing.T) {
		t.Parallel()
		a := assert.New(t)
		cfg := kafka.KafkaConfig{}

		data := kafka2.Message{
			TopicPartition: kafka2.TopicPartition{
				Topic:     nil,
				Partition: 0,
				Offset:    0,
				Error:     nil,
			},
			Key:           nil,
			Value:         []byte(`{"count":0}`),
			Timestamp:     time.Now(),
			TimestampType: 0,
			Opaque:        nil,
			Headers:       nil,
		}

		driver := kafka.NewKafkaDriver(&cfg)
		event, err := driver.ExtractEvent(&data)
		a.Nil(err)
		a.Equal(map[string]string{}, event.Headers)
		a.Equal(&data, event.DriverMessage)
	})
	t.Run("TestKafkaDriver_ExtractEventWithHeaders", func(t *testing.T) {
		t.Parallel()
		a := assert.New(t)
		cfg := kafka.KafkaConfig{}

		data := kafka2.Message{
			TopicPartition: kafka2.TopicPartition{
				Topic:     nil,
				Partition: 0,
				Offset:    0,
				Error:     nil,
			},
			Key:           nil,
			Value:         []byte(`{"count":0}`),
			Timestamp:     time.Now(),
			TimestampType: 0,
			Opaque:        nil,
			Headers: []kafka2.Header{
				{
					Key:   "key",
					Value: []byte("value"),
				},
			},
		}

		driver := kafka.NewKafkaDriver(&cfg)
		event, err := driver.ExtractEvent(&data)
		a.Nil(err)
		a.Equal(map[string]string{"key": "value"}, event.Headers)
		a.Equal(&data, event.DriverMessage)
	})

}
