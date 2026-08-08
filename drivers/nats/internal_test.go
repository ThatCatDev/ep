package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

func TestSanitizeName(t *testing.T) {
	a := assert.New(t)

	a.Equal("anime-db_public_anime", sanitizeName("anime-db.public.anime"))
	a.Equal("a_b", sanitizeName("a*b"))
	a.Equal("a_b", sanitizeName("a>b"))
	a.Equal("a_b", sanitizeName("a b"))
	a.Equal("a_b", sanitizeName("a/b"))
	a.Equal("a_b", sanitizeName("a\\b"))
	a.Equal("a_b", sanitizeName("a\tb"))
	a.Equal("a_b", sanitizeName("a\nb"))
	a.Equal("plain-name", sanitizeName("plain-name"))
	a.Equal("", sanitizeName(""))
}

func TestSubjectMatches(t *testing.T) {
	tests := []struct {
		name    string
		filter  string
		subject string
		want    bool
	}{
		{"exact match", "a.b.c", "a.b.c", true},
		{"different token", "a.b.c", "a.b.d", false},
		{"single wildcard matches one token", "a.*.c", "a.b.c", true},
		{"single wildcard does not span tokens", "a.*", "a.b.c", false},
		{"trailing full wildcard", "a.>", "a.b.c", true},
		{"full wildcard at root", ">", "a.b.c", true},
		{"full wildcard needs at least one token", "a.b.>", "a.b", false},
		{"filter longer than subject", "a.b.c", "a.b", false},
		{"subject longer than filter", "a.b", "a.b.c", false},
		{"wildcard prefix mismatch", "x.>", "a.b", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, subjectMatches(test.filter, test.subject))
		})
	}
}

func TestDeliverPolicy(t *testing.T) {
	a := assert.New(t)

	a.Equal(jetstream.DeliverAllPolicy, deliverPolicy(nil))

	for _, value := range []string{"earliest", "EARLIEST", "beginning", "anything-else"} {
		v := value
		a.Equal(jetstream.DeliverAllPolicy, deliverPolicy(&v), value)
	}

	for _, value := range []string{"latest", "LATEST", "newest", "Newest"} {
		v := value
		a.Equal(jetstream.DeliverNewPolicy, deliverPolicy(&v), value)
	}
}

func TestAckWait(t *testing.T) {
	a := assert.New(t)

	a.Equal(defaultAckWait, (&Config{}).ackWait())
	a.Equal(defaultAckWait, (&Config{AckWait: -time.Second}).ackWait())
	a.Equal(5*time.Second, (&Config{AckWait: 5 * time.Second}).ackWait())
}

func TestConnectOptions(t *testing.T) {
	a := assert.New(t)

	a.Empty(connectOptions(&Config{}))

	clientID := "svc"
	username := "user"
	password := "pass"
	token := "token"
	creds := "/tmp/does-not-need-to-exist.creds"

	opts := connectOptions(&Config{
		ClientID:  &clientID,
		Username:  &username,
		Password:  &password,
		Token:     &token,
		CredsFile: &creds,
	})
	a.Len(opts, 4)

	// Username without a password is not enough to build UserInfo.
	a.Empty(connectOptions(&Config{Username: &username}))
}

func TestConsumerConfig(t *testing.T) {
	a := assert.New(t)

	t.Run("derives a durable name from the consumer group", func(t *testing.T) {
		driver := &NatsDriver{config: &Config{
			ConsumerGroupName: "anime.sync",
			MaxDeliver:        5,
			MaxAckPending:     1,
		}}

		cfg := driver.consumerConfig("anime.public.anime")

		a.Equal("anime_sync", cfg.Durable)
		a.Equal("anime_sync", cfg.Name)
		a.Equal("anime.public.anime", cfg.FilterSubject)
		a.Equal(jetstream.AckExplicitPolicy, cfg.AckPolicy)
		a.Equal(defaultAckWait, cfg.AckWait)
		a.Equal(5, cfg.MaxDeliver)
		a.Equal(1, cfg.MaxAckPending)
	})

	t.Run("leaves the consumer ephemeral when no group is set", func(t *testing.T) {
		driver := &NatsDriver{config: &Config{}}

		cfg := driver.consumerConfig("anime.public.anime")

		a.Empty(cfg.Durable)
		a.Empty(cfg.Name)
	})
}

func TestStreamName(t *testing.T) {
	a := assert.New(t)

	explicit := &NatsDriver{config: &Config{StreamName: "my-stream"}}
	a.Equal("my-stream", explicit.streamName("anime.public.anime"))

	derived := &NatsDriver{config: &Config{}}
	a.Equal("anime_public_anime", derived.streamName("anime.public.anime"))
}

func TestToMessage(t *testing.T) {
	a := assert.New(t)

	msg := toMessage(&fakeMsg{
		subject: "anime.public.anime",
		data:    []byte("payload"),
		headers: map[string][]string{
			KeyHeader: {"anime-1"},
			"retry":   {"3"},
			"empty":   {},
		},
	})

	a.Equal("anime.public.anime", msg.Subject)
	a.Equal([]byte("payload"), msg.Data)
	a.Equal("3", msg.Headers["retry"])
	a.Equal([]byte("anime-1"), msg.Key)
	a.NotContains(msg.Headers, "empty")

	t.Run("leaves the key nil when the header is absent", func(t *testing.T) {
		plain := toMessage(&fakeMsg{subject: "a.b", data: []byte("x")})

		a.Nil(plain.Key)
		a.Empty(plain.Headers)
	})
}
