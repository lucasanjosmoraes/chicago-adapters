package kafkagoadapter

import (
	"github.com/lucasanjosmoraes/chicago-ports/pkg/subscriber"
	"github.com/segmentio/kafka-go"
)

// Message implements subscriber.Message interface.
type Message struct {
	KafkaMessage kafka.Message
}

// NewMessage instantiates an instance of Message, but also helps us to validate
// if Message implements subscriber.Message correctly.
func NewMessage(m kafka.Message) subscriber.Message {
	return Message{
		KafkaMessage: m,
	}
}

// Value returns the value on the KafkaMessage.
func (m Message) Value() []byte {
	return m.KafkaMessage.Value
}

// Headers return an empty map due the lack of support to load headers from others events.
func (m Message) Headers() map[string]string {
	return map[string]string{}
}

// Subject returns the topic defined on the Messages struct.
func (m Message) Subject() string {
	return m.KafkaMessage.Topic
}
