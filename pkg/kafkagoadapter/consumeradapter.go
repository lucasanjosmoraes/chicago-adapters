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

type AuthConfig struct {
	User                string
	Password            string
	TLS                 bool
	SkipTLSVerification bool
}

// ConsumerConfig contains all the attributes required to initialize the Consumer.
type ConsumerConfig struct {
	Auth              AuthConfig
	Brokers           string
	Topic             string
	ConsumerGroup     string
	DL                string
	LogConnections    bool
	HeartbeatInterval time.Duration
	SessionTimeout    time.Duration
	StartOffset       int64
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
	config := kafka.ReaderConfig{
		Brokers:        strings.Split(c.Config.Brokers, ","),
		GroupID:        c.Config.ConsumerGroup,
		Topic:          c.Config.Topic,
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

	dialer := c.getDialer()
	if dialer != nil {
		config.Dialer = dialer
	}

	// TODO: Move *kafka.Reader to ConsumerAdapter.
	r := kafka.NewReader(config)
	defer func() {
		err := r.Close()
		if err != nil {
			c.Logger.Errorf(ctx, "error closing reader connection: %s", err)
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
			c.Logger.Errorf(ctx, "error processing message: %s", err)
			return err
		}

		if err != nil {
			c.Logger.Errorf(ctx, "error processing message: %s", err)
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

func (c ConsumerAdapter) acknowledgeMessage(r *kafka.Reader, m kafka.Message) subscriber.Ack {
	return func(ctx context.Context, logger log.Logger, actions subscriber.Actions) error {
		if actions == nil {
			actions = subscriber.DoNothing
		}

		notifier := func(err error, duration time.Duration) {
			logger.Warnf(ctx, "ack failed, retrying in %s ...", duration)
		}

		err := backoff.RetryNotify(c.commit(ctx, logger, r, m, actions), backoff.NewExponentialBackOff(), notifier)
		if err != nil {
			logger.Errorf(ctx, "error acknowledging message: %s", err)
		}

		return err
	}
}

func (c ConsumerAdapter) commit(ctx context.Context, logger log.Logger, r *kafka.Reader, m kafka.Message, actions subscriber.Actions) backoff.Operation {
	return func() error {
		return commit(ctx, logger, r, m, actions)
	}
}

func commit(ctx context.Context, l log.Logger, r *kafka.Reader, m kafka.Message, actions subscriber.Actions) error {
	actions.BeforeAck(ctx)

	err := r.CommitMessages(ctx, m)
	if err != nil {
		l.Errorf(ctx, "error committing message: %s", err)
		return err
	}

	actions.AfterAck(ctx)
	l.Debug(ctx, "Success committed")

	return nil
}

func (c ConsumerAdapter) rejectMessage(msg subscriber.Message, ack subscriber.Ack) subscriber.Reject {
	return func(ctx context.Context, logger log.Logger, err error, actions subscriber.Actions) {
		if actions == nil {
			actions = subscriber.DoNothing
		}

		defer func() {
			_ = ack(ctx, logger, actions)
		}()

		subscriber.Log(ctx, err, logger)

		sendableErr, ok := err.(subscriber.Error)
		if !ok {
			return
		}
		if !sendableErr.DLSendable() {
			return
		}

		actions.BeforeSendToDL(ctx)

		headers := publisher.ConvertToProducerHeaders(msg.Headers())
		publisherErr := c.Publisher.WriteEvent(ctx, c.Config.DL, publisher.Event{
			Key:     []byte(msg.Subject()),
			Value:   msg.Value(),
			Headers: headers,
		})
		if publisherErr != nil {
			logger.Errorf(ctx, "ignoring message due to error sending message to DLQ: %s", publisherErr)
			return
		}

		actions.AfterSendToDL(ctx)
		logger.Errorf(ctx, "message published on DLQ due to: %s", err)
	}
}

func (c ConsumerAdapter) getDialer() *kafka.Dialer {
	emptyConfig := AuthConfig{}
	if c.Config.Auth == emptyConfig {
		return nil
	}

	mechanism := plain.Mechanism{
		Username: c.Config.Auth.User,
		Password: c.Config.Auth.Password,
	}
	dialer := &kafka.Dialer{
		Timeout:       30 * time.Second,
		SASLMechanism: mechanism,
	}
	if c.Config.Auth.TLS {
		tlsConfig := &tls.Config{}
		if c.Config.Auth.SkipTLSVerification {
			tlsConfig.InsecureSkipVerify = true
		}

		dialer.TLS = tlsConfig
	}

	return dialer
}
