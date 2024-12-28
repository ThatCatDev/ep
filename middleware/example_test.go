package middleware_test

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/thatcatdev/ep/event"
	"github.com/thatcatdev/ep/middleware"
)

type payload struct {
	Count int
}

func ExampleMiddleware() {

	// Create a new chain
	fancyMiddleware := NewFancyMiddleware[payload]()
	chain, err := middleware.Chain[*kafka.Message, payload](fancyMiddleware.Process)

	if err != nil {
		fmt.Println("failed to create middleware")
	}
	// Create a new context
	ctx := context.Background()

	// Create a new data
	datadata := payload{Count: 0}
	data := event.Event[*kafka.Message, payload]{
		Payload: datadata,
	}
	result, err := chain(ctx, data)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(result.Payload.Count)

}

// middleware

type FancyMiddleware[DM any, M any] struct {
}

func NewFancyMiddleware[M any]() *FancyMiddleware[*kafka.Message, M] {
	// do init things here
	return &FancyMiddleware[*kafka.Message, M]{}
}

func (f *FancyMiddleware[DM, M]) Process(ctx context.Context, data event.Event[*kafka.Message, payload], next middleware.Handler[*kafka.Message, payload]) (*event.Event[*kafka.Message, payload], error) {
	// do message processing here
	data.Payload.Count++
	return next(ctx, data)
}
