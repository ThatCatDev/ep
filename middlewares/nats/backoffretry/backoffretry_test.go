package backoffretry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	epnats "github.com/ThatCatDev/ep/v2/drivers/nats"
	"github.com/ThatCatDev/ep/v2/event"
	"github.com/ThatCatDev/ep/v2/internal/mocks"
	"github.com/ThatCatDev/ep/v2/middleware"
	"github.com/ThatCatDev/ep/v2/middlewares/nats/backoffretry"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

type payload struct {
	Count int
}

// fastConfig keeps the backoff sleeps short enough that the tests stay quick.
func fastConfig(maxRetries int) backoffretry.Config {
	return backoffretry.Config{
		MaxRetries:   maxRetries,
		HeaderKey:    "retry",
		RetryQueue:   "anime.retry",
		InitInterval: time.Millisecond,
		MaxInterval:  2 * time.Millisecond,
		Multiplier:   1.5,
	}
}

func newEvent(headers map[string]string) event.Event[*epnats.Message, payload] {
	if headers == nil {
		headers = map[string]string{}
	}

	return event.Event[*epnats.Message, payload]{
		Headers: headers,
		DriverMessage: &epnats.Message{
			Subject: "anime.public.anime",
			Data:    []byte(`{"Count":1}`),
			Headers: headers,
			Key:     []byte("anime-1"),
		},
		Payload: payload{Count: 1},
	}
}

func succeed(_ context.Context, data event.Event[*epnats.Message, payload]) (*event.Event[*epnats.Message, payload], error) {
	return &data, nil
}

func failWith(err error) middleware.Handler[*epnats.Message, payload] {
	return func(_ context.Context, data event.Event[*epnats.Message, payload]) (*event.Event[*epnats.Message, payload], error) {
		return &data, err
	}
}

func TestNewBackoffRetry(t *testing.T) {
	t.Run("builds with an explicit config", func(t *testing.T) {
		a := assert.New(t)
		ctrl := gomock.NewController(t)

		driver := mocks.NewMockDriver[*epnats.Message](ctrl)

		a.NotNil(backoffretry.NewBackoffRetry[payload](driver, fastConfig(3)))
	})

	t.Run("builds with zero-valued intervals", func(t *testing.T) {
		a := assert.New(t)
		ctrl := gomock.NewController(t)

		driver := mocks.NewMockDriver[*epnats.Message](ctrl)

		a.NotNil(backoffretry.NewBackoffRetry[payload](driver, backoffretry.Config{
			MaxRetries: 3,
			HeaderKey:  "retry",
			RetryQueue: "anime.retry",
		}))
	})
}

func TestProcessSuccess(t *testing.T) {
	a := assert.New(t)
	ctrl := gomock.NewController(t)

	driver := mocks.NewMockDriver[*epnats.Message](ctrl)
	// A successful handler must not republish anything.
	driver.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	instance := backoffretry.NewBackoffRetry[payload](driver, fastConfig(3))

	result, err := instance.Process(context.Background(), newEvent(nil), succeed)

	a.NoError(err)
	a.NotNil(result)
	a.Equal(1, result.Payload.Count)
}

func TestProcessRepublishesOnFailure(t *testing.T) {
	t.Run("publishes to the retry subject with a retry count of one", func(t *testing.T) {
		a := assert.New(t)
		ctrl := gomock.NewController(t)

		driver := mocks.NewMockDriver[*epnats.Message](ctrl)

		var produced *epnats.Message

		var producedSubject string

		driver.EXPECT().
			Produce(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, subject string, msg *epnats.Message) error {
				producedSubject = subject
				produced = msg

				return nil
			}).
			Times(1)

		instance := backoffretry.NewBackoffRetry[payload](driver, fastConfig(3))

		result, err := instance.Process(context.Background(), newEvent(nil), failWith(errors.New("boom")))

		a.NoError(err)
		a.Equal("1", result.Headers["retry"])
		a.Equal("anime.retry", producedSubject)
		a.Equal("anime.retry", produced.Subject)
		a.Equal([]byte(`{"Count":1}`), produced.Data)
		a.Equal([]byte("anime-1"), produced.Key)
		a.Equal("1", produced.Headers["retry"])
	})

	t.Run("increments an existing retry count", func(t *testing.T) {
		a := assert.New(t)
		ctrl := gomock.NewController(t)

		driver := mocks.NewMockDriver[*epnats.Message](ctrl)
		driver.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

		instance := backoffretry.NewBackoffRetry[payload](driver, fastConfig(5))

		result, err := instance.Process(
			context.Background(),
			newEvent(map[string]string{"retry": "2"}),
			failWith(errors.New("boom")),
		)

		a.NoError(err)
		a.Equal("3", result.Headers["retry"])
	})

	t.Run("stops republishing once max retries is reached", func(t *testing.T) {
		a := assert.New(t)
		ctrl := gomock.NewController(t)

		driver := mocks.NewMockDriver[*epnats.Message](ctrl)
		driver.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		instance := backoffretry.NewBackoffRetry[payload](driver, fastConfig(3))

		result, err := instance.Process(
			context.Background(),
			newEvent(map[string]string{"retry": "2"}),
			failWith(errors.New("boom")),
		)

		a.NoError(err)
		a.Equal("3", result.Headers["retry"])
	})

	t.Run("returns the error when the retry header is not a number", func(t *testing.T) {
		a := assert.New(t)
		ctrl := gomock.NewController(t)

		driver := mocks.NewMockDriver[*epnats.Message](ctrl)
		driver.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		instance := backoffretry.NewBackoffRetry[payload](driver, fastConfig(3))

		result, err := instance.Process(
			context.Background(),
			newEvent(map[string]string{"retry": "not-a-number"}),
			failWith(errors.New("boom")),
		)

		a.Error(err)
		a.NotNil(result)
	})

	t.Run("surfaces a produce failure", func(t *testing.T) {
		a := assert.New(t)
		ctrl := gomock.NewController(t)

		produceErr := errors.New("nats unavailable")

		driver := mocks.NewMockDriver[*epnats.Message](ctrl)
		driver.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Return(produceErr).Times(1)

		instance := backoffretry.NewBackoffRetry[payload](driver, fastConfig(3))

		_, err := instance.Process(context.Background(), newEvent(nil), failWith(errors.New("boom")))

		a.ErrorIs(err, produceErr)
	})
}

func TestProcessResetsBackoffBetweenFailures(t *testing.T) {
	a := assert.New(t)
	ctrl := gomock.NewController(t)

	driver := mocks.NewMockDriver[*epnats.Message](ctrl)
	driver.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	instance := backoffretry.NewBackoffRetry[payload](driver, fastConfig(10))
	ctx := context.Background()

	// Repeated failures then a success then another failure, to walk the
	// backoff forward and back through Reset.
	for i := 0; i < 3; i++ {
		_, err := instance.Process(ctx, newEvent(nil), failWith(errors.New("boom")))
		a.NoError(err)
	}

	_, err := instance.Process(ctx, newEvent(nil), succeed)
	a.NoError(err)

	_, err = instance.Process(ctx, newEvent(nil), failWith(errors.New("boom")))
	a.NoError(err)
}

func TestProcessInChain(t *testing.T) {
	a := assert.New(t)
	ctrl := gomock.NewController(t)

	driver := mocks.NewMockDriver[*epnats.Message](ctrl)
	driver.EXPECT().Produce(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	instance := backoffretry.NewBackoffRetry[payload](driver, fastConfig(3))

	handler, err := middleware.Chain(
		instance.Process,
		func(ctx context.Context, data event.Event[*epnats.Message, payload], _ middleware.Handler[*epnats.Message, payload]) (*event.Event[*epnats.Message, payload], error) {
			return &data, errors.New("downstream failed")
		},
	)
	a.NoError(err)

	result, err := handler(context.Background(), newEvent(nil))

	a.NoError(err)
	a.Equal("1", result.Headers["retry"])
}
