package backoffretry

import (
	"context"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/ThatCatDev/ep/drivers"
	"github.com/ThatCatDev/ep/event"
	"github.com/ThatCatDev/ep/middleware"
)

type Config struct {
	MaxRetries   int
	HeaderKey    string
	RetryQueue   string
	MaxInterval  time.Duration
	InitInterval time.Duration
	Multiplier   float64
}

type BackoffRetry[M any] struct {
	driver             drivers.Driver[*kafka.Message]
	config             Config
	exponentialBackOff *backoff.ExponentialBackOff
}

func NewBackoffRetry[M any](driver drivers.Driver[*kafka.Message], config Config) *BackoffRetry[M] {
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

func (b *BackoffRetry[M]) Process(ctx context.Context, data event.Event[*kafka.Message, M], next middleware.Handler[*kafka.Message, M]) (*event.Event[*kafka.Message, M], error) {
	// do backoff retry
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

func (b *BackoffRetry[M]) handleError(ctx context.Context, data event.Event[*kafka.Message, M], err error) (*event.Event[*kafka.Message, M], error) {
	// get headers retry as string
	var retryCount int
	retryString := data.Headers[b.config.HeaderKey]
	if retryString == "" {
		retryCount = 0
	} else {
		var err error
		retryCount, err = strconv.Atoi(retryString)
		if err != nil {
			return &data, err
		}
	}

	// do backoff retry
	retryCount++
	data.Headers[b.config.HeaderKey] = strconv.Itoa(retryCount)

	// create kafka headers
	headers := make([]kafka.Header, 0)
	for k, v := range data.Headers {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	if retryCount < b.config.MaxRetries {
		produceErr := b.driver.Produce(ctx, b.config.RetryQueue, &kafka.Message{
			Value:   data.DriverMessage.Value,
			Headers: headers,
			Key:     data.DriverMessage.Key,
		})
		if produceErr != nil {
			return &data, produceErr
		}
	}

	return &data, nil
}
