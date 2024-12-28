package drivers

import "context"

type Driver interface {
	Consume(ctx context.Context, topic string, handler func([]byte) error) error
	Produce(ctx context.Context, topic string, message []byte) error
}
