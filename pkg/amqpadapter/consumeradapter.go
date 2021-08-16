package amqpadapter

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/log"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/publisher"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/subscriber"
	"github.com/streadway/amqp"
)

const requeueHeader = "x-requeue-count"

// ConsumerConfig contains all the attributes required to initialize the Consumer.
type ConsumerConfig struct {
	User          string
	Password      string
	Host          string
	Queue         string
	ConsumerTag   string
	DL            string
	RetryQueue    string
	RetryAttempts int
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

// ConsumerAdapter implements subscriber.Consumer to consume messages from RabbitMQ,
// using the amqp lib.
type ConsumerAdapter struct {
	Producer publisher.Producer
	Logger   log.Logger
	Pool     *ChannelPool
	Queue    amqp.Queue
	MsgChan  <-chan amqp.Delivery
	Config   ConsumerConfig
}

// NewConsumer instantiates a new Consumer, but also helps us to validate
// if ConsumerAdapter implements subscriber.Consumer correctly.
func NewConsumer(ctx context.Context, l log.Logger, p publisher.Producer, c ConsumerConfig, handler subscriber.HandleFunc) (subscriber.Consumer, error) {
	adapter := &ConsumerAdapter{
		Producer: p,
		Logger:   l,
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

	err := adapter.initializeConsumer(ctx)
	if err != nil {
		return nil, err
	}

	adapter.watchConnErrors(ctx, handler)

	return adapter, nil
}

func (c *ConsumerAdapter) initializeConsumer(ctx context.Context) error {
	err := c.Pool.UpdateConnection(ctx, c.Logger)
	if err != nil {
		return err
	}

	return c.setupConsumer(ctx)
}

func (c *ConsumerAdapter) setupConsumer(ctx context.Context) error {
	newQ, err := c.Pool.QueueDeclare(
		ctx, c.Logger, c.Config.Queue, true, false, false, false, nil,
	)
	if err != nil {
		return err
	}
	c.Queue = newQ

	msgs, err := c.Pool.Consume(
		ctx, c.Logger, c.Queue.Name, c.Config.ConsumerTag, false, false, false, false, c.Config.PrefetchCount, c.Config.PrefetchSize, c.Config.Global, nil,
	)
	if err != nil {
		return err
	}
	c.MsgChan = msgs

	return nil
}

func (c ConsumerAdapter) watchConnErrors(ctx context.Context, handler subscriber.HandleFunc) {
	go func() {
		var rabbitErr *amqp.Error

		for {
			rabbitErr = <-c.Pool.ChConnClosed
			time.Sleep(10 * time.Second)
			if rabbitErr == nil {
				c.Logger.Info(ctx, "connection to rabbitMQ has been closed, restarting")
				err := c.setupConsumer(ctx)
				if err != nil {
					c.Logger.Errorf(ctx, "couldn't reconnect to rabbitMQ: %s", err)
					break
				}

				_ = c.Consume(ctx, handler)
			} else {
				c.Logger.Errorf(ctx, "connection to rabbitMQ has been closed due to the error: %s", rabbitErr)
				break
			}
		}
	}()
}

// Consume will pass the received messages to the given handler.
func (c ConsumerAdapter) Consume(ctx context.Context, handler subscriber.HandleFunc) error {
	c.Logger.Infof(ctx, "Start consuming queue %s", c.Queue.Name)

	for m := range c.MsgChan {
		msg := NewMessage(m, c.Config.Queue)
		ack := c.acknowledgeMessage(m)
		handler(msg, ack, c.rejectMessage(msg, ack))
	}

	return nil
}

func (c ConsumerAdapter) acknowledgeMessage(m amqp.Delivery) subscriber.Ack {
	return func(ctx context.Context, logger log.Logger, actions subscriber.Actions) error {
		if actions == nil {
			actions = subscriber.DoNothing
		}

		notifier := func(err error, duration time.Duration) {
			logger.Warnf(ctx, "ack failed, retrying in %s ...", duration)
		}

		err := backoff.RetryNotify(c.commit(ctx, m, actions), backoff.NewExponentialBackOff(), notifier)
		if err != nil {
			logger.Errorf(ctx, "error acknowledging message: %s", err)
		}

		return err
	}
}

func (c ConsumerAdapter) commit(ctx context.Context, m amqp.Delivery, actions subscriber.Actions) backoff.Operation {
	return func() error {
		actions.BeforeAck(ctx)
		defer actions.AfterAck(ctx)

		return m.Ack(false)
	}
}

func (c ConsumerAdapter) rejectMessage(msg subscriber.Message, ack subscriber.Ack) subscriber.Reject {
	return func(ctx context.Context, logger log.Logger, err error, actions subscriber.Actions) {
		if actions == nil {
			actions = subscriber.DoNothing
		}

		subscriber.Log(ctx, err, logger)

		tryToSendToDLQ, retryErr := c.retry(ctx, msg, err, actions)
		if retryErr != nil {
			logger.Errorf(ctx, "sending message to DLQ due to error during retry: %s", retryErr)
			c.sendToDLQ(ctx, logger, err, msg, ack, actions)
			return
		}

		if !tryToSendToDLQ {
			defer func() {
				_ = ack(ctx, logger, actions)
			}()
			return
		}

		c.sendToDLQ(ctx, logger, err, msg, ack, actions)
	}
}

// retry returns a bool indicating if it will be needed to send the message to a DLQ.
func (c ConsumerAdapter) retry(ctx context.Context, msg subscriber.Message, err error, actions subscriber.Actions) (bool, error) {
	if err == nil {
		return false, nil
	}

	retryableErr, ok := err.(subscriber.Error)
	if !ok {
		return true, nil
	}
	if !retryableErr.Retryable() {
		return true, nil
	}

	actions.BeforeRetry(ctx)

	headers := publisher.ConvertToProducerHeaders(msg.Headers())
	shouldRetry := hasAttemptsLeft(c.Config.RetryAttempts, headers)
	if !shouldRetry {
		return true, nil
	}

	headers = increaseAttemptsHeader(headers)
	err = c.Producer.WriteEvent(ctx, "", publisher.Event{
		Key:     []byte(c.Config.RetryQueue),
		Value:   msg.Value(),
		Headers: headers,
	})
	if err != nil {
		return true, err
	}

	actions.AfterRetry(ctx)

	return false, nil
}

func (c ConsumerAdapter) sendToDLQ(ctx context.Context, logger log.Logger, err error, msg subscriber.Message, ack subscriber.Ack, actions subscriber.Actions) {
	defer func() {
		_ = ack(ctx, logger, actions)
	}()

	if err == nil {
		return
	}

	sendableErr, ok := err.(subscriber.Error)
	if !ok {
		return
	}
	if !sendableErr.DLSendable() {
		return
	}

	actions.BeforeSendToDL(ctx)

	publisherErr := c.Producer.WriteEvent(ctx, c.Config.DL, publisher.Event{
		Value: msg.Value(),
	})
	if publisherErr != nil {
		logger.Errorf(ctx, "ignoring message due to error sending message to DLQ: %s", publisherErr)
		return
	}

	actions.AfterSendToDL(ctx)
	logger.Error(ctx, "message published on DLX due to: %s", err)
}

// Stop will close the pool connections.
func (c ConsumerAdapter) Stop(_ context.Context) error {
	if c.Pool != nil {
		return c.Pool.Stop()
	}

	return nil
}

// StopError returns an error from context, if any.
func (c ConsumerAdapter) StopError() error {
	return context.Canceled
}
