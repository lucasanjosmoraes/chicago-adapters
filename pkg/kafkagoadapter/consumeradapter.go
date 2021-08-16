package kafkagoadapter

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/log"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/publisher"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/subscriber"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// KafkaLogger implements kafka-go.Logger to log messages from the lib.
type KafkaLogger struct {
	Logger log.Logger
}

// Printf is the only method support by the kafka-go.Logger interface, and we log
// the messages pass to it at Debug level.
func (l KafkaLogger) Printf(format string, args ...interface{}) {
	l.Logger.Debugf(context.TODO(), format, args...)
}

// ConsumerConfig contains all the attributes required to initialize the Consumer.
type ConsumerConfig struct {
	Brokers             string
	User                string
	Password            string
	Topic               string
	ConsumerGroup       string
	DL                  string
	LogConnections      bool
	TLS                 bool
	SkipTLSVerification bool
	HeartbeatInterval   time.Duration
	SessionTimeout      time.Duration
	StartOffset         int64
}

// ConsumerAdapter implements subscriber.Consumer to consume events from Kafka,
// using the kafka-go lib.
type ConsumerAdapter struct {
	Publisher publisher.Producer
	Logger    log.Logger
	Config    ConsumerConfig
}

// NewConsumer instantiates a new Consumer, but also helps us to validate if
// ConsumerAdapter implements subscriber.Consumer correctly.
func NewConsumer(l log.Logger, p publisher.Producer, c ConsumerConfig) subscriber.Consumer {
	return ConsumerAdapter{
		Publisher: p,
		Logger:    l,
		Config:    c,
	}
}

// Consume will instantiate a new kafka.Reader and pass the received events to
// the given handler.
func (c ConsumerAdapter) Consume(ctx context.Context, handler subscriber.HandleFunc) error {
	mechanism := plain.Mechanism{
		Username: c.Config.User,
		Password: c.Config.Password,
	}
	dialer := &kafka.Dialer{
		Timeout:       30 * time.Second,
		SASLMechanism: mechanism,
	}
	if c.Config.TLS {
		tlsConfig := &tls.Config{}
		if c.Config.SkipTLSVerification {
			tlsConfig.InsecureSkipVerify = true
		}

		dialer.TLS = tlsConfig
	}

	config := kafka.ReaderConfig{
		Brokers:        strings.Split(c.Config.Brokers, ","),
		GroupID:        c.Config.ConsumerGroup,
		Topic:          c.Config.Topic,
		Dialer:         dialer,
		SessionTimeout: 10 * time.Second,
	}
	if c.Config.LogConnections {
		config.Logger = KafkaLogger{Logger: c.Logger}
	}
	if c.Config.HeartbeatInterval > 0 {
		config.HeartbeatInterval = c.Config.HeartbeatInterval
	}
	if c.Config.SessionTimeout > 0 {
		config.SessionTimeout = c.Config.SessionTimeout
	}
	if c.Config.StartOffset > 0 {
		config.StartOffset = c.Config.StartOffset
	}

	// TODO: Move *kafka.Reader to ConsumerAdapter.
	r := kafka.NewReader(config)
	defer func() {
		err := r.Close()
		if err != nil {
			c.Logger.Errorf(ctx, "Error on close reader connection: %s", err)
		}
	}()

	c.Logger.Infof(ctx, "Start consuming topic %s on %s", c.Config.Topic, c.Config.Brokers)
	for {
		m, err := r.FetchMessage(ctx)
		if err == context.Canceled {
			c.Logger.Info(ctx, "Stop consuming")
			break
		}

		if _, ok := err.(net.Error); ok {
			c.Logger.Errorf(ctx, "Error on process message: %s", err)
			return err
		}

		if err != nil {
			c.Logger.Errorf(ctx, "Error on process message: %s", err)
			continue
		}

		c.Logger.Debug(ctx, "Message received")

		message := NewMessage(m)
		ack := c.acknowledgeMessage(r, m)
		handler(message, ack, c.rejectMessage(message, ack))
	}

	return nil
}

// Stop does nothing because we don't need to call any lib method to close the connection.
func (c ConsumerAdapter) Stop(_ context.Context) error {
	return nil
}

// StopError returns an error from context, if any.
func (c ConsumerAdapter) StopError() error {
	return context.Canceled
}

func (c ConsumerAdapter) acknowledgeMessage(r *kafka.Reader, m kafka.Message) func(context.Context) error {
	return func(ctx context.Context) error {
		notifier := func(err error, duration time.Duration) {
			c.Logger.Warnf(ctx, "Ack failed. Retrying in %s ...", duration)
		}

		err := backoff.RetryNotify(backoff.Operation(c.commit(ctx, r, m)), backoff.NewExponentialBackOff(), notifier)
		if err != nil {
			c.Logger.Errorf(ctx, "Error on acknowledge message: %s", err)
		}

		return err
	}
}

func (c ConsumerAdapter) commit(ctx context.Context, r *kafka.Reader, m kafka.Message) func() error {
	return func() error {
		return commit(ctx, c.Logger, r, m)
	}
}

func commit(ctx context.Context, l log.Logger, r *kafka.Reader, m kafka.Message) error {
	err := r.CommitMessages(ctx, m)
	if err != nil {
		l.Errorf(ctx, "Error on commit message: %s", err)
		return err
	}

	l.Debug(ctx, "Success committed")

	return nil
}

func (c ConsumerAdapter) rejectMessage(msg subscriber.Message, ack subscriber.Ack) func(ctx context.Context, err error) {
	return func(ctx context.Context, err error) {
		defer func() {
			_ = ack(ctx)
		}()

		subscriber.Log(ctx, err, c.Logger)

		sendableErr, ok := err.(subscriber.Error)
		if !ok {
			return
		}
		if !sendableErr.DLSendable() {
			return
		}

		headers := publisher.ConvertToProducerHeaders(msg.Headers())
		publisherErr := c.Publisher.WriteEvent(ctx, c.Config.DL, publisher.Event{
			Key:     []byte(msg.Subject()),
			Value:   msg.Value(),
			Headers: headers,
		})
		if publisherErr != nil {
			c.Logger.Errorf(ctx, "Ignore message due to error sending message to DLQ: %s", publisherErr)
			return
		}

		dlErr, ok := err.(subscriber.DLBehavior)
		if !ok {
			c.Logger.Errorf(ctx, "Message published on DLQ due to: %s", err)
			return
		}

		c.Logger.Error(ctx, dlErr.DLErrorMessage())
	}
}
