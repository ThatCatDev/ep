package backoffretry

import (
	"context"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v5"

	"github.com/ThatCatDev/ep/v2/drivers"
	epnats "github.com/ThatCatDev/ep/v2/drivers/nats"
	"github.com/ThatCatDev/ep/v2/event"
	"github.com/ThatCatDev/ep/v2/middleware"
)

// Config mirrors the Kafka backoffretry config field for field, so a service
// swapping drivers only has to change the import and the type parameter.
type Config struct {
	MaxRetries   int
	HeaderKey    string
	RetryQueue   string
	MaxInterval  time.Duration
	InitInterval time.Duration
	Multiplier   float64
}

// BackoffRetry republishes failed messages to a retry subject, incrementing a
// retry counter held in a header, until MaxRetries is reached.
type BackoffRetry[M any] struct {
	driver             drivers.Driver[*epnats.Message]
	config             Config
	exponentialBackOff *backoff.ExponentialBackOff
}

func NewBackoffRetry[M any](driver drivers.Driver[*epnats.Message], config Config) *BackoffRetry[M] {
	maxInterval := backoff.DefaultMaxInterval
	if config.MaxInterval != 0 {
		maxInterval = config.MaxInterval
	}

	initInterval := backoff.DefaultInitialInterval
	if config.InitInterval != 0 {
		initInterval = config.InitInterval
	}

	multiplier := backoff.DefaultMultiplier
	if config.Multiplier != 0 {
		multiplier = config.Multiplier
	}

	return &BackoffRetry[M]{
		driver: driver,
		config: config,
		exponentialBackOff: &backoff.ExponentialBackOff{
			InitialInterval:     initInterval,
			RandomizationFactor: backoff.DefaultRandomizationFactor,
			Multiplier:          multiplier,
			MaxInterval:         maxInterval,
		},
	}
}

func (b *BackoffRetry[M]) Process(ctx context.Context, data event.Event[*epnats.Message, M], next middleware.Handler[*epnats.Message, M]) (*event.Event[*epnats.Message, M], error) {
	duration := b.exponentialBackOff.NextBackOff()
	if duration == backoff.Stop {
		b.exponentialBackOff.Reset()
	}

	newData, err := next(ctx, data)
	if err != nil {
		time.Sleep(duration)

		return b.handleError(ctx, data, err)
	}

	b.exponentialBackOff.Reset()

	return newData, err
}

func (b *BackoffRetry[M]) handleError(ctx context.Context, data event.Event[*epnats.Message, M], _ error) (*event.Event[*epnats.Message, M], error) {
	var retryCount int

	retryString := data.Headers[b.config.HeaderKey]
	if retryString != "" {
		var err error

		retryCount, err = strconv.Atoi(retryString)
		if err != nil {
			return &data, err
		}
	}

	retryCount++
	data.Headers[b.config.HeaderKey] = strconv.Itoa(retryCount)

	if retryCount >= b.config.MaxRetries {
		return &data, nil
	}

	headers := make(map[string]string, len(data.Headers))
	for key, value := range data.Headers {
		headers[key] = value
	}

	produceErr := b.driver.Produce(ctx, b.config.RetryQueue, &epnats.Message{
		Subject: b.config.RetryQueue,
		Data:    data.DriverMessage.Data,
		Headers: headers,
		Key:     data.DriverMessage.Key,
	})
	if produceErr != nil {
		return &data, produceErr
	}

	return &data, nil
}
