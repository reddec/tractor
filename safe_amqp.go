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

type safeMessage struct {
	key string
	msg amqp.Publishing
}

func SafePublisher(retry time.Duration, exchangeName string, exchangeType string, url string, ctx context.Context) (func(topic string, msg amqp.Publishing), <-chan error) {
	var last *safeMessage
	var data = make(chan safeMessage)
	var sf = &SafeExchange{
		ExchangeName: exchangeName,
		ExchangeType: exchangeType,
		Retry:        retry,
		Handler: func(ctx context.Context, ch *amqp.Channel) error {
		LOOP:
			for {
				if last != nil {
					err := ch.Publish(exchangeName, last.key, false, false, last.msg)
					if err != nil {
						return err
					}
					last = nil
				}
				select {
				case m, ok := <-data:
					if !ok {
						break LOOP
					}
					last = &m
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	}
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- sf.Run(ctx, url)
	}()
	return func(topic string, msg amqp.Publishing) {
		select {
		case data <- safeMessage{topic, msg}:
		case <-ctx.Done():
		}
	}, done
}
