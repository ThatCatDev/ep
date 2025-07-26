package pulsar_test

import (
	"context"
	"github.com/ThatCatDev/ep/v2/drivers/pulsar"
	"github.com/ThatCatDev/ep/v2/event"
	"github.com/ThatCatDev/ep/v2/processor"
	"github.com/stretchr/testify/assert"
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

func TestPulsarProcessor(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		a := assert.New(t)
		topicName := "public/default/test.public." + RandomString(10)
		cfg := &pulsar.Config{
			URL:              "pulsar://localhost:6650",
			Topic:            topicName,
			SubscribtionName: RandomString(10),
		}
		driverInstance := pulsar.NewPulsarDriver(cfg)

		type Data struct {
			Count int
		}

		messageReceived := make(chan event.Event[*pulsar.PulsarMessage, Data], 1)

		processorInstance := processor.NewProcessor[*pulsar.PulsarMessage, Data](driverInstance, topicName, func(ctx context.Context, data event.Event[*pulsar.PulsarMessage, Data]) (event.Event[*pulsar.PulsarMessage, Data], error) {
			t.Logf("message received count: %v", data.Payload.Count)
			messageReceived <- data
			return data, nil
		})

		a.NotNil(processorInstance)

		go func() {
			err := processorInstance.Run(context.Background())
			a.Nil(err)
		}()

		msg := &pulsar.PulsarMessage{
			Payload: []byte("{\"count\": 1}"),
		}

		err := driverInstance.Produce(context.Background(), topicName, msg)

		a.Nil(err)

		select {
		case msg := <-messageReceived:
			t.Logf("Test passed: received message '%v'", msg.Payload.Count)
		case <-time.After(5 * time.Second):
			t.Fatalf("Test failed: message was not received within the timeout")
		}
	})

	t.Run("TestProcessorSingleWithHeaders", func(t *testing.T) {
		t.Parallel()
		a := assert.New(t)

		topicName := "public/default/test.public." + RandomString(10)
		cfg := &pulsar.Config{
			URL:              "pulsar://localhost:6650",
			Topic:            topicName,
			SubscribtionName: RandomString(10),
		}
		driverInstance := pulsar.NewPulsarDriver(cfg)

		type Data struct {
			Count int
		}

		messageReceived := make(chan event.Event[*pulsar.PulsarMessage, Data], 1)

		processorInstance := processor.NewProcessor[*pulsar.PulsarMessage, Data](driverInstance, topicName, func(ctx context.Context, data event.Event[*pulsar.PulsarMessage, Data]) (event.Event[*pulsar.PulsarMessage, Data], error) {
			t.Logf("message received count: %v", data.Payload.Count)
			messageReceived <- data
			return data, nil
		})

		a.NotNil(processorInstance)

		go func() {
			err := processorInstance.Run(context.Background())
			a.Nil(err)
		}()

		msg := &pulsar.PulsarMessage{
			Payload: []byte("{\"count\": 1}"),
			Headers: map[string]string{
				"key": "value",
			},
		}

		err := driverInstance.Produce(context.Background(), topicName, msg)

		a.Nil(err)

		select {
		case msg := <-messageReceived:
			t.Logf("Test passed: received message '%v'", msg.Payload.Count)
			a.Equal(1, msg.Payload.Count)
			a.Equal("value", msg.Headers["key"])
		case <-time.After(5 * time.Second):
			t.Fatalf("Test failed: message was not received within the timeout")

		}
	})

	t.Run("Consume multiple messages", func(t *testing.T) {
		a := assert.New(t)
		topicName := "public/default/test.public." + RandomString(10)
		cfg := &pulsar.Config{
			URL:              "pulsar://localhost:6650",
			Topic:            topicName,
			SubscribtionName: RandomString(10),
		}
		driverInstance := pulsar.NewPulsarDriver(cfg)

		type Data struct {
			Count int
		}

		messageReceived := make(chan event.Event[*pulsar.PulsarMessage, Data], 1)

		processorInstance := processor.NewProcessor[*pulsar.PulsarMessage, Data](driverInstance, topicName, func(ctx context.Context, data event.Event[*pulsar.PulsarMessage, Data]) (event.Event[*pulsar.PulsarMessage, Data], error) {
			t.Logf("message received count: %v", data.Payload.Count)
			messageReceived <- data
			return data, nil
		})

		a.NotNil(processorInstance)

		go func() {
			err := processorInstance.Run(context.Background())
			a.Nil(err)
		}()

		for i := 0; i < 1000; i++ {
			msg := &pulsar.PulsarMessage{
				Payload: []byte("{\"count\": 1}"),
			}

			err := driverInstance.Produce(context.Background(), topicName, msg)

			a.Nil(err)
		}

		for i := 0; i < 1000; i++ {
			select {
			case msg := <-messageReceived:
				t.Logf("Test passed: received message '%v'", msg.Payload.Count)
				a.Equal(1, msg.Payload.Count)
			case <-time.After(5 * time.Second):
				t.Fatalf("Test failed: message was not received within the timeout")
			}
		}
	})
}
