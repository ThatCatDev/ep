package nats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fakeMsg is a jetstream.Msg stand-in for exercising the pure conversion
// helpers without a server.
type fakeMsg struct {
	subject string
	data    []byte
	headers nats.Header
}

var _ jetstream.Msg = (*fakeMsg)(nil)

func (f *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return &jetstream.MsgMetadata{}, nil }
func (f *fakeMsg) Data() []byte                              { return f.data }
func (f *fakeMsg) Headers() nats.Header                      { return f.headers }
func (f *fakeMsg) Subject() string                           { return f.subject }
func (f *fakeMsg) Reply() string                             { return "" }
func (f *fakeMsg) Ack() error                                { return nil }
func (f *fakeMsg) DoubleAck(context.Context) error           { return nil }
func (f *fakeMsg) Nak() error                                { return nil }
func (f *fakeMsg) NakWithDelay(time.Duration) error          { return nil }
func (f *fakeMsg) InProgress() error                         { return nil }
func (f *fakeMsg) Term() error                               { return nil }
func (f *fakeMsg) TermWithReason(string) error               { return nil }
