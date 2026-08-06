//go:build integration_nats

package nats_test

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	epnats "github.com/ThatCatDev/ep/v2/drivers/nats"
	"github.com/ThatCatDev/ep/v2/event"
	"github.com/ThatCatDev/ep/v2/middlewares/nats/backoffretry"
	"github.com/ThatCatDev/ep/v2/processor"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

const natsURL = "nats://localhost:4222"

type Data struct {
	Count int
}

func RandomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz"

	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}

	return string(b)
}

// requireServer fails fast with an actionable message rather than letting
// every subtest time out separately. Reaching this file at all means the
// integration_nats tag was passed, so a missing broker is an error, not a
// reason to skip.
func requireServer(t *testing.T) {
	t.Helper()

	conn, err := nats.Connect(natsURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("nats is not reachable at %s (run `docker compose up -d nats`): %v", natsURL, err)
	}

	conn.Close()
}

func TestNatsDriver(t *testing.T) {
	requireServer(t)

	t.Run("Consume one message", func(t *testing.T) {
		a := assert.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		cfg := &epnats.Config{
			URL:               natsURL,
			ConsumerGroupName: RandomString(10),
			AckWait:           2 * time.Second,
		}
		driverInstance := epnats.NewNatsDriver(cfg)
		defer driverInstance.Close() //nolint:errcheck

		a.NotNil(driverInstance)

		subject := "integration." + RandomString(10)
		messageReceived := make(chan string, 1)

		go func() {
			//nolint:errcheck
			_ = driverInstance.Consume(ctx, subject, func(_ context.Context, _ *epnats.Message, payload []byte) error {
				t.Logf("Received message: %s", string(payload))
				messageReceived <- string(payload)

				return nil
			})
		}()

		err := driverInstance.Produce(ctx, subject, &epnats.Message{
			Data:    []byte(`{"Count": 1}`),
			Headers: map[string]string{"trace-id": "integration"},
		})
		a.NoError(err)

		select {
		case msg := <-messageReceived:
			a.JSONEq(`{"Count": 1}`, msg)
		case <-ctx.Done():
			t.Fatal("timed out waiting for message")
		}
	})

	t.Run("Durable consumer resumes after reconnect", func(t *testing.T) {
		a := assert.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		group := RandomString(10)
		subject := "durable." + RandomString(10)

		producer := epnats.NewNatsDriver(&epnats.Config{URL: natsURL})
		a.NoError(producer.CreateTopic(ctx, subject))
		a.NoError(producer.Produce(ctx, subject, &epnats.Message{Data: []byte(`{"Count": 1}`)}))
		a.NoError(producer.Close())

		// A fresh driver with the same group name picks up the backlog,
		// because the durable consumer's position lives on the server.
		consumer := epnats.NewNatsDriver(&epnats.Config{
			URL:               natsURL,
			ConsumerGroupName: group,
			AckWait:           2 * time.Second,
		})
		defer consumer.Close() //nolint:errcheck

		received := make(chan string, 1)

		go func() {
			//nolint:errcheck
			_ = consumer.Consume(ctx, subject, func(_ context.Context, _ *epnats.Message, payload []byte) error {
				received <- string(payload)

				return nil
			})
		}()

		select {
		case msg := <-received:
			a.JSONEq(`{"Count": 1}`, msg)
		case <-ctx.Done():
			t.Fatal("durable consumer did not receive the backlog")
		}
	})
}

func TestNatsProcessor(t *testing.T) {
	requireServer(t)

	t.Run("Success", func(t *testing.T) {
		a := assert.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		subject := "processor." + RandomString(10)
		cfg := &epnats.Config{
			URL:               natsURL,
			ConsumerGroupName: RandomString(10),
			AckWait:           2 * time.Second,
		}
		driverInstance := epnats.NewNatsDriver(cfg)
		defer driverInstance.Close() //nolint:errcheck

		messageReceived := make(chan event.Event[*epnats.Message, Data], 1)

		processorInstance := processor.NewProcessor[*epnats.Message, Data](
			driverInstance,
			subject,
			func(_ context.Context, data event.Event[*epnats.Message, Data]) (event.Event[*epnats.Message, Data], error) {
				t.Logf("message received count: %v", data.Payload.Count)
				messageReceived <- data

				return data, nil
			},
		)

		a.NotNil(processorInstance)

		go func() {
			//nolint:errcheck
			_ = processorInstance.Run(ctx)
		}()

		err := driverInstance.Produce(ctx, subject, &epnats.Message{
			Data: []byte(`{"Count": 1}`),
			Key:  []byte("anime-1"),
		})
		a.NoError(err)

		select {
		case data := <-messageReceived:
			a.Equal(1, data.Payload.Count)
			a.Equal(subject, data.DriverMessage.Subject)
			a.Equal([]byte("anime-1"), data.DriverMessage.Key)
		case <-ctx.Done():
			t.Fatal("timed out waiting for the processor to handle the message")
		}
	})

	t.Run("Failed messages land on the retry subject", func(t *testing.T) {
		a := assert.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()

		subject := "retry." + RandomString(10)
		retrySubject := subject + "-retry"

		cfg := &epnats.Config{
			URL:               natsURL,
			ConsumerGroupName: RandomString(10),
			AckWait:           2 * time.Second,
		}
		driverInstance := epnats.NewNatsDriver(cfg)
		defer driverInstance.Close() //nolint:errcheck

		a.NoError(driverInstance.CreateTopic(ctx, subject))
		a.NoError(driverInstance.CreateTopic(ctx, retrySubject))

		retryMiddleware := backoffretry.NewBackoffRetry[Data](driverInstance, backoffretry.Config{
			MaxRetries:   3,
			HeaderKey:    "retry",
			RetryQueue:   retrySubject,
			InitInterval: 10 * time.Millisecond,
			MaxInterval:  50 * time.Millisecond,
		})

		var once sync.Once

		processorInstance := processor.NewProcessor[*epnats.Message, Data](
			driverInstance,
			subject,
			func(_ context.Context, data event.Event[*epnats.Message, Data]) (event.Event[*epnats.Message, Data], error) {
				return data, errors.New("processing failed")
			},
		).AddMiddleware(retryMiddleware.Process)

		go func() {
			once.Do(func() {
				//nolint:errcheck
				_ = processorInstance.Run(ctx)
			})
		}()

		// Watch the retry subject with a separate driver so the two
		// subscriptions do not share a durable consumer.
		watcher := epnats.NewNatsDriver(&epnats.Config{
			URL:               natsURL,
			ConsumerGroupName: RandomString(10),
			AckWait:           2 * time.Second,
		})
		defer watcher.Close() //nolint:errcheck

		retried := make(chan *epnats.Message, 1)

		go func() {
			//nolint:errcheck
			_ = watcher.Consume(ctx, retrySubject, func(_ context.Context, msg *epnats.Message, _ []byte) error {
				retried <- msg

				return nil
			})
		}()

		err := driverInstance.Produce(ctx, subject, &epnats.Message{
			Data: []byte(`{"Count": 7}`),
			Key:  []byte("anime-7"),
		})
		a.NoError(err)

		select {
		case msg := <-retried:
			a.Equal("1", msg.Headers["retry"])
			a.JSONEq(`{"Count": 7}`, string(msg.Data))
			a.Equal([]byte("anime-7"), msg.Key)
		case <-ctx.Done():
			t.Fatal("failed message never reached the retry subject")
		}
	})
}
