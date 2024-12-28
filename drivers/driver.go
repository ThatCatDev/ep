package drivers

import (
	"context"
	"github.com/thatcatdev/ep/event"
)

type Driver[DM any] interface {
	Consume(ctx context.Context, topic string, handler func(context.Context, DM, []byte) error) error
	Produce(ctx context.Context, topic string, message []byte) error
	CreateTopic(ctx context.Context, topic string) error
	Close() error
	ExtractEvent(data DM) (*event.SubData[DM], error)
}
