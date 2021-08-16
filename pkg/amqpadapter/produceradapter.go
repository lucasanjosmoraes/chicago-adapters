package amqpadapter

import (
	"context"
	"fmt"

	"github.com/lucasanjosmoraes/chicago-ports/pkg/log"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/publisher"
	"github.com/streadway/amqp"
)

// DelayedQueue is used when we want to send the message back to the queue after
// a time interval.
type DelayedQueue struct {
	Name string
	Args map[string]interface{}
}

// ProducerConfig contains all the attributes required to initialize the Producer.
type ProducerConfig struct {
	User         string
	Password     string
	Host         string
	DelayedQueue *DelayedQueue
}

// ProducerAdapter implements publisher.Producer to produce messages on RabbitMQ exchanges,
// using the amqp lib.
type ProducerAdapter struct {
	Logger log.Logger
	Pool   *ChannelPool
	Queue  amqp.Queue
	Config ProducerConfig
}

// NewProducer instantiates a new ProducerAdapter, but also helps us to validate
// if ProducerAdapter implements publisher.Producer correctly.
func NewProducer(ctx context.Context, l log.Logger, c ProducerConfig) (publisher.Producer, error) {
	adapter := &ProducerAdapter{
		Logger: l,
		Pool: &ChannelPool{
			URL: fmt.Sprintf(
				"amqp://%s:%s@%s/",
				c.User,
				c.Password,
				c.Host,
			),
		},
		Config: c,
	}

	err := adapter.initializeProducer(ctx)
	if err != nil {
		return nil, err
	}

	return adapter, nil
}

func (p *ProducerAdapter) initializeProducer(ctx context.Context) error {
	err := p.Pool.UpdateConnection(ctx, p.Logger)
	if err != nil {
		return err
	}

	return p.setupProducer(ctx)
}

func (p *ProducerAdapter) setupProducer(ctx context.Context) error {
	if p.Config.DelayedQueue == nil {
		return nil
	}

	queue, err := p.Pool.QueueDeclare(ctx, p.Logger, p.Config.DelayedQueue.Name, true, false, false, false, p.Config.DelayedQueue.Args)
	if err != nil {
		return err
	}
	p.Queue = queue

	return nil
}

func transformToAMQPHeaders(hs []publisher.EventHeader) amqp.Table {
	if len(hs) == 0 {
		return nil
	}

	amqpHs := make(amqp.Table)
	for _, h := range hs {
		amqpHs[h.Key] = h.Value
	}

	return amqpHs
}

// WriteEvent publish a given event using the pool coordinator.
func (p ProducerAdapter) WriteEvent(ctx context.Context, topic string, event publisher.Event) error {
	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        event.Value,
		Headers:     transformToAMQPHeaders(event.Headers),
	}
	routingKey := string(event.Key)

	return p.Pool.Produce(ctx, p.Logger, topic, routingKey, msg)
}

// WriteEvents publish multiple events using the pool coordinator, one event at a time.
func (p ProducerAdapter) WriteEvents(ctx context.Context, topic string, events []publisher.Event) error {
	var msg amqp.Publishing
	var routingKey string
	var err error

	for _, event := range events {
		msg = amqp.Publishing{
			ContentType: "application/json",
			Body:        event.Value,
			Headers:     transformToAMQPHeaders(event.Headers),
		}
		routingKey = string(event.Key)

		err = p.Pool.Produce(ctx, p.Logger, topic, routingKey, msg)
		if err != nil {
			return err
		}
	}

	return nil
}

// Stop will close the pool connections.
func (p ProducerAdapter) Stop(_ context.Context) error {
	if p.Pool != nil {
		return p.Pool.Stop()
	}

	return nil
}

// StopError returns an error from context, if any.
func (p ProducerAdapter) StopError() error {
	return context.Canceled
}
