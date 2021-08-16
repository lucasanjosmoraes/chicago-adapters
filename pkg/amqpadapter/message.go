package amqpadapter

import (
	"fmt"

	"github.com/lucasanjosmoraes/chicago-ports/pkg/subscriber"
	"github.com/streadway/amqp"
)

// Message implements subscriber.Message interface.
type Message struct {
	AMQPMessage amqp.Delivery
	Queue       string
}

// NewMessage instantiates a new Message, but also helps us to validate
// if Message implements subscriber.Message correctly.
func NewMessage(m amqp.Delivery, q string) subscriber.Message {
	return Message{
		AMQPMessage: m,
		Queue:       q,
	}
}

// Value returns the body on the AMQPMessage.
func (m Message) Value() []byte {
	return m.AMQPMessage.Body
}

// Headers returns a map with all the headers contained in the AMQPMessage.
func (m Message) Headers() map[string]string {
	headers := make(map[string]string)

	for k, v := range m.AMQPMessage.Headers {
		switch value := v.(type) {
		case []byte:
			headers[k] = string(value)
		default:
			headers[k] = fmt.Sprintf("%v", v)
		}
	}

	return headers
}

// Subject returns the queue defined on the Messages struct.
func (m Message) Subject() string {
	return m.Queue
}
