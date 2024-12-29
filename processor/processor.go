package processor

import (
	"context"

	"github.com/thatcatdev/ep/drivers"
	"github.com/thatcatdev/ep/event"
	"github.com/thatcatdev/ep/middleware"
)

type Process[DM any, M any] func(ctx context.Context, data event.Event[DM, M]) (event.Event[DM, M], error)

type Processor[DM any, M any] interface {
	Run(ctx context.Context) error
	AddMiddleware(m middleware.Middleware[DM, M]) Processor[DM, M]
}

type processor[DM any, M any] struct {
	driver      drivers.Driver[DM]
	topic       string
	process     Process[DM, M]
	middlewares []middleware.Middleware[DM, M]
}

func NewProcessor[DM any, M any](driver drivers.Driver[DM], topic string, process Process[DM, M]) Processor[DM, M] {
	return &processor[DM, M]{
		process: process,
		topic:   topic,
		driver:  driver,
	}
}

func (p *processor[DM, M]) Run(ctx context.Context) error {
	// Consume messages from the queue
	err := p.driver.Consume(ctx, p.topic, func(ctx context.Context, originalMessage DM, message []byte) error {
		extractedData, err := p.driver.ExtractEvent(originalMessage)
		if err != nil {
			return err
		}
		// Transform the message into the expected structure
		data := &event.Event[DM, M]{
			Headers:       extractedData.Headers,
			DriverMessage: originalMessage,
			RawData:       extractedData.RawData,
		}
		if err := data.Transform(message); err != nil {
			return err
		}

		middlewares := append(p.middlewares, func(ctx context.Context, data event.Event[DM, M], next middleware.Handler[DM, M]) (*event.Event[DM, M], error) {
			_, err = p.process(ctx, data)
			if err != nil {
				return &data, err
			}

			return next(ctx, data)
		})
		chain, err := middleware.Chain[DM, M](middlewares...)
		if err != nil {
			return err
		}

		data, err = chain(ctx, *data)
		if err != nil {
			return err
		}

		return nil
	})

	return err
}

func (p *processor[DM, M]) AddMiddleware(m middleware.Middleware[DM, M]) Processor[DM, M] {
	p.middlewares = append(p.middlewares, m)

	return p
}
