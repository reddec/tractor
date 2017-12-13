package tractor

import (
	"context"
	"github.com/streadway/amqp"
	"time"
	"net"
	"github.com/pkg/errors"
	"github.com/twinj/uuid"
)

func Call(url string, flow, event string, data []byte, ctx context.Context) ([]byte, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial: func(network, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "connect to broker")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "open channel")
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return nil, errors.Wrap(err, "declare reply queue")
	}

	corrId := uuid.NewV4().String()

	err = ch.Publish(NormalizeName(flow), event, true, false, amqp.Publishing{
		ReplyTo:       q.Name,
		CorrelationId: corrId,
		Body:          data,
		MessageId:     corrId,
	})

	if err != nil {
		return nil, errors.Wrap(err, "send request")
	}

	cons, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		return nil, errors.Wrap(err, "open consumer")
	}

	select {
	case msg, ok := <-cons:
		if !ok {
			return nil, errors.New("consumer channel closed")
		}
		return msg.Body, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
