package amqpadapter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/log"
	"github.com/streadway/amqp"
)

// ChannelPool helps the adapters on this package to manage its connections with
// RabbitMQ, to declare queues and to use features of the amqp lib with retry.
type ChannelPool struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	ChConnClosed chan *amqp.Error
	mux          sync.Mutex
	URL          string
}

// UpdateConnection will try to create a new connection, if need.
func (p *ChannelPool) UpdateConnection(ctx context.Context, l log.Logger) error {
	p.mux.Lock()
	defer p.mux.Unlock()

	if p.conn != nil && !p.conn.IsClosed() {
		return nil
	}

	create := func() error {
		if p.conn != nil {
			_ = p.conn.Close()
		}

		con, err := amqp.Dial(p.URL)
		if err != nil {
			return err
		}

		p.ChConnClosed = make(chan *amqp.Error)
		_ = con.NotifyClose(p.ChConnClosed)
		p.conn = con

		ch, err := p.conn.Channel()
		if err != nil {
			return fmt.Errorf("amqp create channel error: %s", err)
		}

		p.ch = ch

		return nil
	}

	notifier := func(err error, duration time.Duration) {
		l.Warnf(ctx, "connecting to AMQP failed. Retrying in %s seconds...", duration)
	}

	err := backoff.RetryNotify(create, backoff.NewExponentialBackOff(), notifier)
	if err != nil {
		l.Errorf(ctx, "error on connection to AMQP: %s", err)
	}

	return nil
}

// QueueDeclare will declare a queue with the given attributes, which it can create
// if it doesn't exist.
func (p *ChannelPool) QueueDeclare(ctx context.Context, l log.Logger, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	newQ, err := p.ch.QueueDeclare(
		name, durable, autoDelete, exclusive, noWait, args,
	)
	if err == nil {
		return newQ, nil
	}

	l.Info(ctx, "error on declare queue. Trying to update AMQP Connection")
	err = p.UpdateConnection(ctx, l)
	if err != nil {
		return amqp.Queue{}, err
	}

	return p.ch.QueueDeclare(
		name, durable, autoDelete, exclusive, noWait, args,
	)
}

// Consume will call for the Consume method of the amqp.Channel and will try to create
// a new connection if receive an error on the first call.
func (p *ChannelPool) Consume(ctx context.Context, l log.Logger, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, prefetchCount, prefetchSize int, global bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	err := p.ch.Qos(prefetchCount, prefetchSize, global)
	if err != nil {
		l.Info(ctx, "error on consume. Trying to update AMQP connection")
		err = p.UpdateConnection(ctx, l)
		if err != nil {
			return nil, err
		}

		err = p.ch.Qos(prefetchCount, prefetchSize, global)
		if err != nil {
			return nil, err
		}
	}

	return p.ch.Consume(
		queue, consumer, autoAck, exclusive, noLocal, noWait, args,
	)
}

// Produce will call for the Produce method of the amqp.Channel and will try to create
// a new connection if receive an error on the first call.
func (p *ChannelPool) Produce(ctx context.Context, l log.Logger, exchange, routingKey string, msg amqp.Publishing) error {
	err := p.ch.Publish(exchange, routingKey, false, false, msg)
	if err == nil {
		return nil
	}

	l.Info(ctx, "error on publish. Trying to update AMQP Connection")
	err = p.UpdateConnection(ctx, l)
	if err != nil {
		return err
	}

	return p.ch.Publish(exchange, routingKey, false, false, msg)
}

// Stop will close the connections with the RabbitMQ.
func (p *ChannelPool) Stop() error {
	p.mux.Lock()
	defer p.mux.Unlock()

	if p.conn == nil {
		return nil
	}

	return p.conn.Close()
}
