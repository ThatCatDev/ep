# Event Processor (EP)

The Event Processor (EP) is a component of the [Event-Driven Architecture (EDA)](https://en.wikipedia.org/wiki/Event-driven_architecture)
that processes events and performs actions based on those events. The EP is responsible for processing events. The EP 
can be used to implement business logic, data processing, and other tasks that require event processing. EP supports
multiple drivers such as Kafka and RabbitMQ with the option to add middlewares such as a tracing
middleware, metrics middleware, backoff retry, and more. Some specific middlewares such as a backoff retry handled by
Rabbitmq is also possible abstracting the retry logic from the business logic.


## Features

- **Event Processing**: Process events from different sources such as Kafka and RabbitMQ.
- **Middleware Support**: Add middlewares to the event processor to add additional functionality such as tracing, 
- metrics, and backoff retry.
- **Driver Support**: Supports multiple drivers such as Kafka and RabbitMQ.


## Usage

Processor is created by defining the original message from the driver (in this case kafka.Message, for RabbitMQ it 
would be amqp.Delivery) and defining the payload, this payload type could later be used with avro to verify the payload.

```go
type payload struct {
    Count int
}

processorInstance := processor.NewProcessor[*kafka.Message, payload](driver, "test-topic", func(ctx context.Context, data event.Event[*kafka2.Message, payload]) (event.Event[*kafka2.Message, payload], error) {
    log.Println("Processing data")
    log.Printf("Data: %+v", data)
    return data, nil
})
```

Process an event without middlewares:

```go
package main

import (
	"context"
	"log"
    
	kafka2 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	
    "github.com/ThatCatDev/ep/v2/drivers/kafka"
    "github.com/ThatCatDev/ep/v2/event"
    "github.com/ThatCatDev/ep/v2/processor"
)

type payload struct {
	Count int
}

func main() {
	ctx := context.Background()
	// define driver config else where
	config := kafka.KafkaConfig{
		ConsumerGroupName:        consumerGroupName,
		BootstrapServers:         brokerHost,
		SaslMechanism:            nil,
		SecurityProtocol:         nil,
		Username:                 nil,
		Password:                 nil,
		ConsumerSessionTimeoutMs: nil,
		ConsumerAutoOffsetReset:  nil,
		ClientID:                 nil,
		Debug:                    nil,
	}
	driver := kafka.NewKafkaDriver(config)
	defer driver.Close()

	// create topics yourself
	err := driver.CreateTopic(ctx, topicName)
	if err != nil {
		log.Fatalf("failed to create topic: %v", err)
	}

	// create processor instance with function. for Kafka use *kafka.Message and for rabbitmq use amqp.Delivery
	processorInstance := processor.NewProcessor[*kafka.Message, payload](driver, "test-topic", func(ctx context.Context, data event.Event[*kafka2.Message, payload]) (event.Event[*kafka2.Message, payload], error) {
		log.Println("Processing data")
		log.Printf("Data: %+v", data)
		return data, nil
	})

	err = processorInstance.Run(ctx)
	
    if err != nil {
        log.Fatalf("failed to run processor: %v", err)
    }
	
}

```

Adding middlewares to the processor:

```go
err := processorInstance.
	AddMiddleware(middleware.NewMetricsMiddleware()).
    AddMiddleware(middleware.NewTracingMiddleware()).
    AddMiddleware(middleware.NewBackoffRetryMiddleware()).
    Run(ctx)
```

### Creating Drivers

Drivers are simple to create, they must implement this interface:

```go
type Driver[DM any] interface {
	Consume(ctx context.Context, topic string, handler func(context.Context, DM, []byte) error) error
	Produce(ctx context.Context, topic string, message []byte) error
	CreateTopic(ctx context.Context, topic string) error
	Close() error
	ExtractEvent(data DM) (*event.SubData[DM], error)
}
```

example kafka consumer function:

```go
func (k *KafkaDriver) Consume(ctx context.Context, topic string, handler func(context.Context, *kafka.Message, []byte) error) error {
	cfg := GetKafkaConsumerConfig(*k.kafkaConfig)
	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return err
	}

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		return err
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			return err
		}

		if err := handler(ctx, msg, msg.Value); err != nil {
			return err
		}
	}
}
```


### Middlewares

To create a middleware, you just need to implement the below type:
```go
type Middleware[DM any, M any] func(ctx context.Context, data event.Event[DM, M], next Handler[DM, M]) (*event.Event[DM, M], error)
```

Example of a middleware, you can specify *kafka.Message since this below middleware is only used for kafka:

```go
// middleware

type FancyMiddleware[DM any, M any] struct {
}

func NewFancyMiddleware[M any]() *FancyMiddleware[*kafka.Message, M] {
	// do init things here
	return &FancyMiddleware[*kafka.Message, M]{}
}

func (f *FancyMiddleware[DM, M]) Process(ctx context.Context, data event.Event[*kafka.Message, payload], next middleware.Handler[*kafka.Message, payload]) (*event.Event[*kafka.Message, payload], error) {
	// do message processing here
	data.Payload.Count++
	return next(ctx, data)
}

```