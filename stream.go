package tractor

import (
	"context"
	"github.com/streadway/amqp"
	"github.com/twinj/uuid"
	"time"
)

func (c *Config) RunStreamPublisher(ctx context.Context, url string) error {
	publisher, done := SafePublisher(c.Reconnect, c.exchangeName(), "topic", url, ctx)
LOOP:
	for {
		output := c.RunStream(ctx)
		for line := range output {
			msg := amqp.Publishing{
				MessageId: uuid.NewV4().String(),
				Timestamp: time.Now(),
				Body:      []byte(line),
			}
			msg.Headers = make(amqp.Table)
			msg.Headers[ServiceHeader] = c.Name
			msg.Headers[ServiceFromHeader] = c.Name
			publisher(c.Event, msg)
		}
		select {
		case <-time.After(c.Reconnect):
		case <-ctx.Done():
			break LOOP
		}
	}
	return <-done
}
