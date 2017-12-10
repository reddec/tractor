package tractor

import (
	"github.com/streadway/amqp"
	"time"
	"context"
	"github.com/pkg/errors"
	"log"
	"net"
)

type SafeExchange struct {
	Retry        time.Duration
	ExchangeName string
	ExchangeType string
	Handler      func(ctx context.Context, ch *amqp.Channel) error
}

func (m *SafeExchange) createExchange(ch *amqp.Channel) (error) {
	return ch.ExchangeDeclare(m.ExchangeName, m.ExchangeType, true, false, false, false, nil)
}

func (m *SafeExchange) runWithConnection(ctx context.Context, conn *amqp.Connection) error {
	for {
		ch, err := conn.Channel()
		if err != nil {
			return errors.Wrap(err, "open channel")
		}
		err = ch.Qos(1, 0, true)
		if err != nil {
			return errors.Wrap(err, "failed set prefetch size")
		}
		err = m.createExchange(ch)
		if err != nil {
			log.Println("failed create infrastracture:", err)
			goto CLOSE
		}

		err = m.Handler(ctx, ch)
		if err != nil {
			log.Println("failed consume:", err)
			goto CLOSE
		}

	CLOSE:
		ch.Close()
		select {
		case <-time.After(m.Retry):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

}

func (m *SafeExchange) Run(ctx context.Context, url string) error {
	for {
		conn, err := amqp.DialConfig(url, amqp.Config{
			Heartbeat: 10 * time.Second,
			Locale:    "en_US",
			Dial: func(network, addr string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		})

		if err != nil {
			log.Println("failed connect:", err)
		} else {
			err = m.runWithConnection(ctx, conn)
			if err != nil {
				log.Println("failed run:", err)
			}
			conn.Close()
		}
		select {
		case <-time.After(m.Retry):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
