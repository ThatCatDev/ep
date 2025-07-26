package pulsar

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ThatCatDev/ep/v2/drivers"
	"github.com/ThatCatDev/ep/v2/event"

	"github.com/apache/pulsar-client-go/pulsar"
)

type PulsarMessage struct {
	Payload []byte
	Headers map[string]string
}

type PulsarDriver struct {
	client   pulsar.Client
	consumer pulsar.Consumer
	producer pulsar.Producer
}

func NewPulsarDriver(config *Config) drivers.Driver[*PulsarMessage] {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: config.URL,
	})
	if err != nil {
		panic(err)
	}

	return &PulsarDriver{
		client: client,
	}
}

func (p *PulsarDriver) Consume(ctx context.Context, topic string, handler func(context.Context, *PulsarMessage, []byte) error) error {
	if p.consumer == nil {
		consumer, err := p.client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: "my-sub",
		})
		if err != nil {
			return err
		}

		p.consumer = consumer
	}

	defer p.consumer.Close()

	for {
		msg, err := p.consumer.Receive(ctx)
		if err != nil {
			return err
		}

		puslarMessage := &PulsarMessage{
			Payload: msg.Payload(),
			Headers: msg.Properties(),
		}

		if err := handler(ctx, puslarMessage, msg.Payload()); err != nil {
			return err
		}

		if err := p.consumer.Ack(msg); err != nil {
			log.Println("failed to ack message: ", err)
		}
	}
}

func (p *PulsarDriver) Produce(ctx context.Context, topic string, message *PulsarMessage) error {
	if p.producer == nil {
		producer, err := p.client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
		})
		if err != nil {
			return err
		}

		p.producer = producer
	}

	// convert pulsar.Message to producer.Message
	msg := &pulsar.ProducerMessage{
		Payload:    message.Payload,
		Properties: message.Headers,
	}

	if _, err := p.producer.Send(ctx, msg); err != nil {
		return err
	}

	return nil
}

func (p *PulsarDriver) CreateTopic(ctx context.Context, topic string) error {
	return nil
}

func (p *PulsarDriver) Close() error {
	p.consumer.Close()
	p.producer.Close()
	p.client.Close()

	return nil
}

func (p *PulsarDriver) ExtractEvent(msg *PulsarMessage) (*event.SubData[*PulsarMessage], error) {
	eventData := &event.SubData[*PulsarMessage]{
		DriverMessage: msg,
	}

	eventData.Headers = msg.Headers

	msgByte, err := json.Marshal(*msg)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(msgByte, &eventData.RawData)
	if err != nil {
		return nil, err
	}

	return eventData, nil
}
