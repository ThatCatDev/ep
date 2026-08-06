package nats_test

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	epnats "github.com/ThatCatDev/ep/v2/drivers/nats"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startServer boots an in-process JetStream server so the driver tests need no
// docker daemon. Integration tests under integration/nats cover the driver
// against a real deployment.
func startServer(t *testing.T) string {
	t.Helper()

	srv, err := natsserver.NewServer(&natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	require.NoError(t, err)

	go srv.Start()

	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server did not become ready")
	}

	t.Cleanup(srv.Shutdown)

	return srv.ClientURL()
}

func randomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz"

	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}

	return string(b)
}

func TestNew(t *testing.T) {
	t.Run("returns an error when config is nil", func(t *testing.T) {
		a := assert.New(t)

		driver, err := epnats.New(nil)

		a.Nil(driver)
		a.Error(err)
	})

	t.Run("returns an error when the server is unreachable", func(t *testing.T) {
		a := assert.New(t)

		driver, err := epnats.New(&epnats.Config{URL: "nats://127.0.0.1:1"})

		a.Nil(driver)
		a.Error(err)
	})

	t.Run("connects to a running server", func(t *testing.T) {
		a := assert.New(t)

		driver, err := epnats.New(&epnats.Config{URL: startServer(t)})

		a.NoError(err)
		a.NotNil(driver)
		a.NoError(driver.Close())
	})

	t.Run("applies auth and client id options", func(t *testing.T) {
		a := assert.New(t)

		clientID := "ep-test"
		token := "not-the-server-token"

		driver, err := epnats.New(&epnats.Config{
			URL:      startServer(t),
			ClientID: &clientID,
			Token:    &token,
		})

		// The test server has no auth configured, so a token is simply
		// ignored; the point here is that the options are wired up at all.
		a.NoError(err)
		a.NotNil(driver)
		a.NoError(driver.Close())
	})
}

func TestNewNatsDriver(t *testing.T) {
	t.Run("returns a usable driver", func(t *testing.T) {
		a := assert.New(t)

		driver := epnats.NewNatsDriver(&epnats.Config{URL: startServer(t)})

		a.NotNil(driver)
		a.NoError(driver.Close())
	})

	t.Run("panics when the server is unreachable", func(t *testing.T) {
		a := assert.New(t)

		a.Panics(func() {
			epnats.NewNatsDriver(&epnats.Config{URL: "nats://127.0.0.1:1"})
		})
	})
}

func TestCreateTopic(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	driver := epnats.NewNatsDriver(&epnats.Config{URL: startServer(t)})
	defer driver.Close() //nolint:errcheck

	subject := "orders." + randomString(8)

	a.NoError(driver.CreateTopic(ctx, subject))

	// Creating the same subject twice is a no-op rather than an error.
	a.NoError(driver.CreateTopic(ctx, subject))
}

func TestProduceAndConsume(t *testing.T) {
	t.Run("round trips payload, headers and key", func(t *testing.T) {
		a := assert.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		driver := epnats.NewNatsDriver(&epnats.Config{
			URL:               startServer(t),
			ConsumerGroupName: randomString(8),
			AckWait:           time.Second,
		})
		defer driver.Close() //nolint:errcheck

		subject := "anime." + randomString(8)
		received := make(chan *epnats.Message, 1)

		go func() {
			//nolint:errcheck
			_ = driver.Consume(ctx, subject, func(_ context.Context, msg *epnats.Message, payload []byte) error {
				a.Equal(msg.Data, payload)
				received <- msg

				return nil
			})
		}()

		err := driver.Produce(ctx, subject, &epnats.Message{
			Data:    []byte(`{"count":1}`),
			Headers: map[string]string{"trace-id": "abc123"},
			Key:     []byte("anime-42"),
		})
		a.NoError(err)

		select {
		case msg := <-received:
			a.Equal(subject, msg.Subject)
			a.JSONEq(`{"count":1}`, string(msg.Data))
			a.Equal("abc123", msg.Headers["trace-id"])
			a.Equal([]byte("anime-42"), msg.Key)
		case <-ctx.Done():
			t.Fatal("timed out waiting for message")
		}
	})

	t.Run("delivers messages in publish order", func(t *testing.T) {
		a := assert.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		driver := epnats.NewNatsDriver(&epnats.Config{
			URL:               startServer(t),
			ConsumerGroupName: randomString(8),
			AckWait:           time.Second,
			MaxAckPending:     1,
		})
		defer driver.Close() //nolint:errcheck

		subject := "ordered." + randomString(8)
		const total = 10

		for i := 0; i < total; i++ {
			err := driver.Produce(ctx, subject, &epnats.Message{
				Data: []byte{byte('0' + i)},
			})
			a.NoError(err)
		}

		got := make(chan string, total)

		go func() {
			//nolint:errcheck
			_ = driver.Consume(ctx, subject, func(_ context.Context, msg *epnats.Message, _ []byte) error {
				got <- string(msg.Data)

				return nil
			})
		}()

		for i := 0; i < total; i++ {
			select {
			case value := <-got:
				a.Equal(string([]byte{byte('0' + i)}), value)
			case <-ctx.Done():
				t.Fatalf("timed out after %d messages", i)
			}
		}
	})

	t.Run("returns an error when the message is nil", func(t *testing.T) {
		a := assert.New(t)

		driver := epnats.NewNatsDriver(&epnats.Config{URL: startServer(t)})
		defer driver.Close() //nolint:errcheck

		a.Error(driver.Produce(context.Background(), "some.subject", nil))
	})

	t.Run("shares one stream across subjects when StreamName is set", func(t *testing.T) {
		a := assert.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		streamName := "shared" + randomString(6)
		driver := epnats.NewNatsDriver(&epnats.Config{
			URL:               startServer(t),
			StreamName:        streamName,
			ConsumerGroupName: randomString(8),
			AckWait:           time.Second,
		})
		defer driver.Close() //nolint:errcheck

		first := "shared." + randomString(6)
		second := "shared." + randomString(6)

		a.NoError(driver.CreateTopic(ctx, first))
		a.NoError(driver.CreateTopic(ctx, second))

		a.NoError(driver.Produce(ctx, second, &epnats.Message{Data: []byte("second")}))

		received := make(chan []byte, 1)

		go func() {
			//nolint:errcheck
			_ = driver.Consume(ctx, second, func(_ context.Context, msg *epnats.Message, _ []byte) error {
				received <- msg.Data

				return nil
			})
		}()

		select {
		case data := <-received:
			a.Equal("second", string(data))
		case <-ctx.Done():
			t.Fatal("timed out waiting for message on the shared stream")
		}
	})
}

func TestConsumeHandlerError(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	group := randomString(8)
	url := startServer(t)

	driver := epnats.NewNatsDriver(&epnats.Config{
		URL:               url,
		ConsumerGroupName: group,
		AckWait:           time.Second,
	})
	defer driver.Close() //nolint:errcheck

	subject := "failing." + randomString(8)
	handlerErr := errors.New("handler blew up")

	a.NoError(driver.Produce(ctx, subject, &epnats.Message{Data: []byte("payload")}))

	err := driver.Consume(ctx, subject, func(context.Context, *epnats.Message, []byte) error {
		return handlerErr
	})
	a.ErrorIs(err, handlerErr)

	// The failed message was nacked rather than acked, so the same durable
	// consumer gets it again on the next pass.
	redelivered := make(chan []byte, 1)

	go func() {
		//nolint:errcheck
		_ = driver.Consume(ctx, subject, func(_ context.Context, msg *epnats.Message, _ []byte) error {
			redelivered <- msg.Data

			return nil
		})
	}()

	select {
	case data := <-redelivered:
		a.Equal("payload", string(data))
	case <-ctx.Done():
		t.Fatal("message was not redelivered after the handler failed")
	}
}

func TestConsumeCancellation(t *testing.T) {
	t.Run("returns nil when the context is cancelled", func(t *testing.T) {
		a := assert.New(t)

		driver := epnats.NewNatsDriver(&epnats.Config{
			URL:               startServer(t),
			ConsumerGroupName: randomString(8),
		})
		defer driver.Close() //nolint:errcheck

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)

		go func() {
			done <- driver.Consume(ctx, "idle."+randomString(8), func(context.Context, *epnats.Message, []byte) error {
				return nil
			})
		}()

		// Give the consumer a moment to block on Next before cancelling.
		time.Sleep(250 * time.Millisecond)
		cancel()

		select {
		case err := <-done:
			a.NoError(err)
		case <-time.After(10 * time.Second):
			t.Fatal("Consume did not return after cancellation")
		}
	})

	t.Run("uses an ephemeral consumer when no group is configured", func(t *testing.T) {
		a := assert.New(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		driver := epnats.NewNatsDriver(&epnats.Config{URL: startServer(t), AckWait: time.Second})
		defer driver.Close() //nolint:errcheck

		subject := "ephemeral." + randomString(8)
		a.NoError(driver.Produce(ctx, subject, &epnats.Message{Data: []byte("hello")}))

		received := make(chan []byte, 1)

		go func() {
			//nolint:errcheck
			_ = driver.Consume(ctx, subject, func(_ context.Context, msg *epnats.Message, _ []byte) error {
				received <- msg.Data

				return nil
			})
		}()

		select {
		case data := <-received:
			a.Equal("hello", string(data))
		case <-ctx.Done():
			t.Fatal("timed out waiting for message")
		}
	})
}

func TestConsumeAutoOffsetReset(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	latest := "latest"
	driver := epnats.NewNatsDriver(&epnats.Config{
		URL:                     startServer(t),
		ConsumerGroupName:       randomString(8),
		ConsumerAutoOffsetReset: &latest,
		AckWait:                 time.Second,
	})
	defer driver.Close() //nolint:errcheck

	subject := "latest." + randomString(8)

	// Published before the consumer exists, so "latest" must skip it.
	a.NoError(driver.Produce(ctx, subject, &epnats.Message{Data: []byte("old")}))

	received := make(chan []byte, 2)

	go func() {
		//nolint:errcheck
		_ = driver.Consume(ctx, subject, func(_ context.Context, msg *epnats.Message, _ []byte) error {
			received <- msg.Data

			return nil
		})
	}()

	time.Sleep(500 * time.Millisecond)
	a.NoError(driver.Produce(ctx, subject, &epnats.Message{Data: []byte("new")}))

	select {
	case data := <-received:
		a.Equal("new", string(data))
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
}

func TestClose(t *testing.T) {
	a := assert.New(t)

	driver := epnats.NewNatsDriver(&epnats.Config{URL: startServer(t)})

	a.NoError(driver.Close())
	// Closing twice is safe.
	a.NoError(driver.Close())

	// Operations after Close fail rather than panicking on a nil connection.
	a.Error(driver.Produce(context.Background(), "some.subject", &epnats.Message{Data: []byte("x")}))
	a.Error(driver.CreateTopic(context.Background(), "some.subject"))
	a.Error(driver.Consume(context.Background(), "some.subject", func(context.Context, *epnats.Message, []byte) error {
		return nil
	}))
}

func TestExtractEvent(t *testing.T) {
	driver := epnats.NewNatsDriver(&epnats.Config{URL: startServer(t)})
	defer driver.Close() //nolint:errcheck

	t.Run("populates headers, raw data and the driver message", func(t *testing.T) {
		a := assert.New(t)

		msg := &epnats.Message{
			Subject: "anime.public.anime",
			Data:    []byte(`{"count":1}`),
			Headers: map[string]string{"retry": "2"},
			Key:     []byte("anime-1"),
		}

		subData, err := driver.ExtractEvent(msg)

		a.NoError(err)
		a.Equal(msg, subData.DriverMessage)
		a.Equal("2", subData.Headers["retry"])
		a.Equal("anime.public.anime", subData.RawData["Subject"])
	})

	t.Run("tolerates a message with no headers", func(t *testing.T) {
		a := assert.New(t)

		subData, err := driver.ExtractEvent(&epnats.Message{Data: []byte("{}")})

		a.NoError(err)
		a.NotNil(subData.Headers)
		a.Empty(subData.Headers)
	})

	t.Run("returns an error when the message is nil", func(t *testing.T) {
		a := assert.New(t)

		subData, err := driver.ExtractEvent(nil)

		a.Nil(subData)
		a.Error(err)
	})
}

func TestStreamNameForSubject(t *testing.T) {
	a := assert.New(t)

	a.Equal("anime-db_public_anime", epnats.StreamNameForSubject("anime-db.public.anime"))
	a.Equal("orders", epnats.StreamNameForSubject("orders"))
	a.Equal("a_b_c", epnats.StreamNameForSubject("a.b>c"))
}
