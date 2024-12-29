package backoffretry_test

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/thatcatdev/ep/drivers/mocks"
	"github.com/thatcatdev/ep/event"
	"github.com/thatcatdev/ep/middlewares/kafka/backoffretry"
	"go.uber.org/mock/gomock"
	"testing"
)

type payload struct {
	Count int
}

func TestNewBackoffRetry(t *testing.T) {
	t.Run("TestNewBackoffRetry", func(t *testing.T) {
		t.Parallel()

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
		t.Parallel()
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

		a.NotNil(err)

		a.Equal("1", messageEvent.Headers["retry"])

		messageEvent, err = backoffretryInstance.Process(context.Background(), *messageEvent, func(ctx context.Context, data event.Event[*kafka.Message, payload]) (*event.Event[*kafka.Message, payload], error) {
			return &data, fmt.Errorf("error")
		})

		a.NotNil(err)

		a.Equal("2", messageEvent.Headers["retry"])

		messageEvent, err = backoffretryInstance.Process(context.Background(), *messageEvent, func(ctx context.Context, data event.Event[*kafka.Message, payload]) (*event.Event[*kafka.Message, payload], error) {
			return &data, fmt.Errorf("error")
		})

		a.NotNil(err)

		a.Equal("3", messageEvent.Headers["retry"])

	})
}
