package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ThatCatDev/ep/v2/drivers"
	"github.com/ThatCatDev/ep/v2/event"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// KeyHeader carries Message.Key across the wire, since NATS has no
	// first-class message key.
	KeyHeader = "Ep-Key"

	defaultAckWait = 30 * time.Second
)

// Message is the driver-level representation of a NATS message.
//
// Note on ordering: NATS has no partition key. JetStream orders messages
// within a subject, and a consumer with MaxAckPending > 1 may hand two
// messages from the same subject to different workers concurrently. Key is
// carried as a header so key-aware middleware and downstream sinks keep
// working across a Kafka swap, but unlike a Kafka partition key it does not
// influence delivery order. Where per-key ordering matters, either give each
// key its own subject or make the consumer's work order-independent.
type Message struct {
	Subject string
	Data    []byte
	Headers map[string]string
	Key     []byte
}

// NatsDriver implements drivers.Driver on top of NATS JetStream.
type NatsDriver struct {
	config *Config

	mu      sync.Mutex
	conn    *nats.Conn
	js      jetstream.JetStream
	ensured map[string]struct{}
}

// NewNatsDriver connects to NATS and returns a driver, panicking on failure.
// It matches the constructor style of the Kafka and Pulsar drivers; use New
// when you would rather handle the connection error yourself.
func NewNatsDriver(config *Config) drivers.Driver[*Message] {
	driver, err := New(config)
	if err != nil {
		panic(err)
	}

	return driver
}

// New connects to NATS and returns a driver.
func New(config *Config) (drivers.Driver[*Message], error) {
	if config == nil {
		return nil, errors.New("nats: config is nil")
	}

	conn, err := nats.Connect(config.URL, connectOptions(config)...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("failed to create jetstream context: %w", err)
	}

	return &NatsDriver{
		config:  config,
		conn:    conn,
		js:      js,
		ensured: map[string]struct{}{},
	}, nil
}

func connectOptions(config *Config) []nats.Option {
	opts := make([]nats.Option, 0, 4)

	if config.ClientID != nil {
		opts = append(opts, nats.Name(*config.ClientID))
	}
	if config.Username != nil && config.Password != nil {
		opts = append(opts, nats.UserInfo(*config.Username, *config.Password))
	}
	if config.Token != nil {
		opts = append(opts, nats.Token(*config.Token))
	}
	if config.CredsFile != nil {
		opts = append(opts, nats.UserCredentials(*config.CredsFile))
	}

	return opts
}

// Consume binds a durable consumer to subject and invokes handler for each
// message, acking only once the handler returns nil.
//
// A handler error stops consumption and is returned to the caller, matching
// the Kafka driver. The failed message is explicitly nacked so JetStream
// redelivers it immediately rather than after AckWait expires.
func (n *NatsDriver) Consume(ctx context.Context, topic string, handler func(context.Context, *Message, []byte) error) error {
	stream, err := n.ensureStream(ctx, topic)
	if err != nil {
		return err
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, n.consumerConfig(topic))
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	iter, err := consumer.Messages()
	if err != nil {
		return fmt.Errorf("failed to open message iterator: %w", err)
	}
	defer iter.Stop()

	// iter.Next blocks with no context of its own; stopping the iterator is
	// what unblocks it, so cancellation has to be plumbed through manually.
	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			iter.Stop()
		case <-done:
		}
	}()

	for {
		msg, err := iter.Next()
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) || ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf("read error: %w", err)
		}

		message := toMessage(msg)
		if err := handler(ctx, message, message.Data); err != nil {
			//nolint:errcheck
			_ = msg.Nak()

			return err
		}

		if err := msg.Ack(); err != nil {
			return fmt.Errorf("ack error: %w", err)
		}
	}
}

// Produce publishes message to subject, creating the backing stream if it does
// not exist yet.
func (n *NatsDriver) Produce(ctx context.Context, topic string, message *Message) error {
	if message == nil {
		return errors.New("nats: message is nil")
	}

	if _, err := n.ensureStream(ctx, topic); err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: topic,
		Data:    message.Data,
		Header:  nats.Header{},
	}

	for key, value := range message.Headers {
		msg.Header.Set(key, value)
	}
	if len(message.Key) > 0 {
		msg.Header.Set(KeyHeader, string(message.Key))
	}

	if _, err := n.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// CreateTopic ensures a JetStream stream exists that captures subject.
//
// NATS subjects need no creation of their own; what has to exist is a stream
// binding the subject, which is what this creates.
func (n *NatsDriver) CreateTopic(ctx context.Context, topic string) error {
	_, err := n.ensureStream(ctx, topic)

	return err
}

// Close drains in-flight messages and closes the connection.
func (n *NatsDriver) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.conn == nil {
		return nil
	}

	err := n.conn.Drain()
	n.conn = nil
	n.js = nil

	if err != nil {
		return fmt.Errorf("failed to drain connection: %w", err)
	}

	return nil
}

// ExtractEvent converts a driver message into the generic event envelope.
func (n *NatsDriver) ExtractEvent(data *Message) (*event.SubData[*Message], error) {
	if data == nil {
		return nil, errors.New("nats: message is nil")
	}

	eventData := &event.SubData[*Message]{
		DriverMessage: data,
		Headers:       data.Headers,
	}

	if eventData.Headers == nil {
		eventData.Headers = map[string]string{}
	}

	msgByte, err := json.Marshal(*data)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(msgByte, &eventData.RawData); err != nil {
		return nil, err
	}

	return eventData, nil
}

func (n *NatsDriver) consumerConfig(subject string) jetstream.ConsumerConfig {
	cfg := jetstream.ConsumerConfig{
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: deliverPolicy(n.config.ConsumerAutoOffsetReset),
		AckWait:       n.config.ackWait(),
		MaxDeliver:    n.config.MaxDeliver,
		MaxAckPending: n.config.MaxAckPending,
	}

	// An empty durable name yields an ephemeral consumer, which is the right
	// behaviour when no group was configured.
	if name := sanitizeName(n.config.ConsumerGroupName); name != "" {
		cfg.Durable = name
		cfg.Name = name
	}

	return cfg
}

func (n *NatsDriver) ensureStream(ctx context.Context, subject string) (jetstream.Stream, error) {
	name := n.streamName(subject)

	n.mu.Lock()
	js := n.js
	_, alreadyEnsured := n.ensured[subject]
	n.mu.Unlock()

	if js == nil {
		return nil, errors.New("nats: driver is closed")
	}

	stream, err := js.Stream(ctx, name)
	switch {
	case err == nil:
		if alreadyEnsured {
			return stream, nil
		}

		return n.bindSubject(ctx, js, stream, subject)
	case errors.Is(err, jetstream.ErrStreamNotFound):
		stream, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     name,
			Subjects: []string{subject},
			MaxAge:   n.config.StreamMaxAge,
			Replicas: n.config.StreamReplicas,
		})
		if err != nil {
			// A racing producer may have created it in the meantime.
			if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
				return n.ensureStream(ctx, subject)
			}

			return nil, fmt.Errorf("failed to create stream %q: %w", name, err)
		}

		n.markEnsured(subject)

		return stream, nil
	default:
		return nil, fmt.Errorf("failed to look up stream %q: %w", name, err)
	}
}

// bindSubject adds subject to an existing stream's subject list when it is not
// already covered, so that a shared stream can back several subjects.
func (n *NatsDriver) bindSubject(ctx context.Context, js jetstream.JetStream, stream jetstream.Stream, subject string) (jetstream.Stream, error) {
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read stream info: %w", err)
	}

	for _, existing := range info.Config.Subjects {
		if existing == subject || subjectMatches(existing, subject) {
			n.markEnsured(subject)

			return stream, nil
		}
	}

	cfg := info.Config
	cfg.Subjects = append(cfg.Subjects, subject)

	updated, err := js.UpdateStream(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to bind subject %q to stream %q: %w", subject, cfg.Name, err)
	}

	n.markEnsured(subject)

	return updated, nil
}

func (n *NatsDriver) markEnsured(subject string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.ensured[subject] = struct{}{}
}

func (n *NatsDriver) streamName(subject string) string {
	if n.config.StreamName != "" {
		return n.config.StreamName
	}

	return StreamNameForSubject(subject)
}

// StreamNameForSubject derives a legal stream name from a subject. Stream
// names may not contain the characters that give subjects their meaning, so
// "anime-db.public.anime" becomes "anime-db_public_anime".
func StreamNameForSubject(subject string) string {
	return sanitizeName(subject)
}

func sanitizeName(name string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '.', '*', '>', ' ', '\t', '\n', '/', '\\':
			return '_'
		default:
			return r
		}
	}, name)
}

// subjectMatches reports whether a stream subject filter, which may contain
// the "*" and ">" wildcards, already covers subject.
func subjectMatches(filter, subject string) bool {
	filterTokens := strings.Split(filter, ".")
	subjectTokens := strings.Split(subject, ".")

	for i, token := range filterTokens {
		if token == ">" {
			return i <= len(subjectTokens)-1
		}
		if i >= len(subjectTokens) {
			return false
		}
		if token != "*" && token != subjectTokens[i] {
			return false
		}
	}

	return len(filterTokens) == len(subjectTokens)
}

func deliverPolicy(autoOffsetReset *string) jetstream.DeliverPolicy {
	if autoOffsetReset == nil {
		return jetstream.DeliverAllPolicy
	}

	switch strings.ToLower(*autoOffsetReset) {
	case "latest", "newest":
		return jetstream.DeliverNewPolicy
	default:
		return jetstream.DeliverAllPolicy
	}
}

func toMessage(msg jetstream.Msg) *Message {
	headers := map[string]string{}
	for key, values := range msg.Headers() {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	message := &Message{
		Subject: msg.Subject(),
		Data:    msg.Data(),
		Headers: headers,
	}

	if key, ok := headers[KeyHeader]; ok {
		message.Key = []byte(key)
	}

	return message
}
