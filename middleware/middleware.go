package middleware

import (
	"context"

	"github.com/ThatCatDev/ep/event"
)

type Handler[DM any, M any] func(ctx context.Context, data event.Event[DM, M]) (*event.Event[DM, M], error)

type Middleware[DM any, M any] func(ctx context.Context, data event.Event[DM, M], next Handler[DM, M]) (*event.Event[DM, M], error)

func Chain[DM any, M any](middlewares ...Middleware[DM, M]) (Handler[DM, M], error) {
	var finalHandler Handler[DM, M] = func(ctx context.Context, data event.Event[DM, M]) (*event.Event[DM, M], error) {
		return &data, nil
	}

	for i := len(middlewares) - 1; i >= 0; i-- {
		mw := middlewares[i]

		next := finalHandler

		finalHandler = func(ctx context.Context, data event.Event[DM, M]) (*event.Event[DM, M], error) {
			return mw(ctx, data, next)
		}
	}

	return finalHandler, nil
}
