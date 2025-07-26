package pulsar_test

import (
	"context"
	"github.com/ThatCatDev/ep/v2/drivers/pulsar"
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

func TestPulsarDriver(t *testing.T) {
	t.Run("Consume one message", func(t *testing.T) {
		a := assert.New(t)
		cfg := &pulsar.Config{
			URL:              "pulsar://localhost:6650",
			Topic:            "public/default/test.public." + RandomString(10),
			SubscribtionName: RandomString(10),
		}
		driverInstance := pulsar.NewPulsarDriver(cfg)

		a.NotNil(driverInstance)

		// create channel to signal message consumption
		messageReceived := make(chan string, 1)
		ctx := context.Background()
		go func() {
			err := driverInstance.Consume(ctx, cfg.Topic, func(ctx context.Context, msg *pulsar.PulsarMessage, payload []byte) error {
				t.Logf("Received message: %s", string(payload))
				messageReceived <- string(payload)
				return nil
			})
			a.Nil(err)
		}()

		msg := &pulsar.PulsarMessage{
			Payload: []byte("test"),
		}
		err := driverInstance.Produce(ctx, cfg.Topic, msg)

		a.Nil(err)

		// check if message was received
		select {
		case msg := <-messageReceived:
			t.Logf("Test passed: received message '%s'", msg)
		case <-time.After(5 * time.Second):
			t.Fatalf("Test failed: message was not received within the timeout")
		}
	})
	t.Run("Consume multiple messages", func(t *testing.T) {
		a := assert.New(t)
		cfg := &pulsar.Config{
			URL:              "pulsar://localhost:6650",
			Topic:            "public/default/test.public." + RandomString(10),
			SubscribtionName: RandomString(10),
		}
		driverInstance := pulsar.NewPulsarDriver(cfg)

		a.NotNil(driverInstance)

		// create channel to signal message consumption
		messageReceived := make(chan string, 1)
		ctx := context.Background()
		go func() {
			err := driverInstance.Consume(ctx, cfg.Topic, func(ctx context.Context, msg *pulsar.PulsarMessage, payload []byte) error {
				t.Logf("Received message: %s", string(payload))
				messageReceived <- string(payload)
				return nil
			})
			a.Nil(err)
		}()

		for i := 0; i < 1000; i++ {
			msg := &pulsar.PulsarMessage{
				Payload: []byte("test"),
			}
			err := driverInstance.Produce(ctx, cfg.Topic, msg)
			a.Nil(err)

		}

		// check if message was received
		for i := 0; i < 1000; i++ {
			select {
			case msg := <-messageReceived:
				t.Logf("Test passed: received message '%s'", msg)
			case <-time.After(5 * time.Second):
				t.Fatalf("Test failed: message was not received within the timeout")
			}
		}

	})

}
