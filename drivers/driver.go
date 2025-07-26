//go:generate mockgen -source=driver.go -destination=mocks/driver.go -package mocks
package drivers

import (
	"context"

	"github.com/ThatCatDev/ep/v2/event"
)

type Driver[DM any] interface {
	Consume(ctx context.Context, topic string, handler func(context.Context, DM, []byte) error) error
	Produce(ctx context.Context, topic string, message DM) error
	CreateTopic(ctx context.Context, topic string) error
	Close() error
	ExtractEvent(data DM) (*event.SubData[DM], error)
}
