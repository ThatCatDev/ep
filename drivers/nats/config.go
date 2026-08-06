package nats

import "time"

// Config configures the NATS JetStream driver.
//
// The field names deliberately echo the Kafka driver's config so that a
// service swapping drivers has an obvious mapping for each setting. Where the
// two systems genuinely differ the doc comment says so rather than pretending
// the concepts line up.
type Config struct {
	// URL is the NATS server URL, e.g. "nats://localhost:4222". Multiple
	// servers may be given as a comma-separated list.
	URL string `default:"nats://localhost:4222" env:"NATSURL"`

	// ConsumerGroupName is the durable consumer name, and is the closest
	// equivalent to a Kafka consumer group: every driver instance using this
	// name shares one subscription's workload and ack state. When empty the
	// consumer is ephemeral and its position is not retained across restarts.
	ConsumerGroupName string `env:"NATSCONSUMERGROUPNAME"`

	// StreamName is the JetStream stream backing the subjects this driver
	// produces to and consumes from. When empty, a name is derived from the
	// subject by StreamNameForSubject.
	StreamName string `env:"NATSSTREAMNAME"`

	// ConsumerAutoOffsetReset mirrors the Kafka setting of the same name and
	// only applies when a consumer is first created. "earliest" delivers
	// everything the stream still retains, "latest" only what is published
	// afterwards. Defaults to "earliest".
	ConsumerAutoOffsetReset *string `env:"NATSCONSUMERAUTOOFFSETRESET"`

	// ClientID sets the connection name the server reports in monitoring.
	ClientID *string `env:"NATSCLIENTID"`

	// Username and Password authenticate the connection. Token and CredsFile
	// are the other two supported schemes; set at most one scheme.
	Username *string `env:"NATSUSERNAME"`
	Password *string `env:"NATSPASSWORD"`
	Token    *string `env:"NATSTOKEN"`

	// CredsFile is the path to a NATS credentials file (JWT + seed).
	CredsFile *string `env:"NATSCREDSFILE"`

	// AckWait is how long JetStream waits for an ack before redelivering a
	// message. Defaults to 30s.
	AckWait time.Duration

	// MaxDeliver caps how many times a message is redelivered before
	// JetStream gives up on it. Zero means unlimited, which is the JetStream
	// default. Prefer the backoffretry middleware for poison-message handling
	// so failures land in an inspectable retry subject rather than vanishing.
	MaxDeliver int

	// MaxAckPending bounds how many messages may be in flight unacked. Set it
	// to 1 to process a subject strictly in order at the cost of throughput;
	// see the ordering note on Message.Key. Zero uses the JetStream default.
	MaxAckPending int

	// StreamMaxAge is the retention window for streams this driver creates.
	// Zero keeps messages until the stream's size limits evict them.
	StreamMaxAge time.Duration

	// StreamReplicas is the replica count for streams this driver creates.
	// Zero or 1 means unreplicated; use 3 against a clustered deployment.
	StreamReplicas int
}

func (c *Config) ackWait() time.Duration {
	if c.AckWait <= 0 {
		return defaultAckWait
	}

	return c.AckWait
}
