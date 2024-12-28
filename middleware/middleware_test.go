package middleware_test

import (
	"context"
	"fmt"
	"github.com/thatcatdev/ep/middleware"

	"github.com/stretchr/testify/assert"
	"github.com/thatcatdev/ep/event"
	"testing"
)

func TestMiddlewareProcess(t *testing.T) {
	t.Run("TestMiddlewareProcess", func(t *testing.T) {
		t.Parallel()
		a := assert.New(t)

		type DriverMessage struct {
		}
		type Message struct {
			Count int
		}

		// Create a new middleware
		middleware1 := func(ctx context.Context, data event.Event[DriverMessage, Message], next middleware.Handler[DriverMessage, Message]) (*event.Event[DriverMessage, Message], error) {
			data.Payload.Count++
			return next(ctx, data)
		}

		middleware2 := func(ctx context.Context, data event.Event[DriverMessage, Message], next middleware.Handler[DriverMessage, Message]) (*event.Event[DriverMessage, Message], error) {
			data.Payload.Count += 2
			return next(ctx, data)
		}

		// Create a new chain
		chain, err := middleware.Chain[DriverMessage, Message](middleware1, middleware2)

		if err != nil {
			t.Fatalf("failed to create middleware")
		}
		// Create a new context
		ctx := context.Background()

		// Create a new data
		datadata := Message{Count: 0}
		data := event.Event[DriverMessage, Message]{Payload: datadata}

		// Process the data
		result, err := chain(ctx, data)
		a.Nil(err)

		// Assert the result
		a.Equal(3, result.Payload.Count)

	})
	t.Run("TestMiddlewareProcessWithShortCircuit", func(t *testing.T) {
		t.Parallel()
		a := assert.New(t)

		type DriverMessage struct {
		}

		type Message struct {
			Count int
		}

		// Create a new middleware
		middleware1 := func(ctx context.Context, data event.Event[DriverMessage, Message], next middleware.Handler[DriverMessage, Message]) (*event.Event[DriverMessage, Message], error) {
			data.Payload.Count++
			return &data, fmt.Errorf("short-circuit")
		}

		middleware2 := func(ctx context.Context, data event.Event[DriverMessage, Message], next middleware.Handler[DriverMessage, Message]) (*event.Event[DriverMessage, Message], error) {
			data.Payload.Count += 2
			return &data, nil
		}

		// Create a new chain
		chain, err := middleware.Chain[DriverMessage, Message](middleware1, middleware2)

		if err != nil {
			t.Fatalf("failed to create middleware")
		}
		// Create a new context
		ctx := context.Background()

		// Create a new data
		datadata := Message{Count: 0}
		data := event.Event[DriverMessage, Message]{Payload: datadata}

		// Process the data
		result, err := chain(ctx, data)
		a.NotNil(err)

		// Assert the result
		a.Equal(1, result.Payload.Count)

	})
}
