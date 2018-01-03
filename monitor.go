package tractor

import (
	"github.com/streadway/amqp"
	"time"
	"context"
	"github.com/pkg/errors"
	"log"
	"fmt"
)

type MonitorPrinter func(tm time.Time, event string, msgId string, msg []byte, headers map[string]string)

type Monitor struct {
	Flow   string
	Retry  time.Duration
	Events []string
}

func (c *Monitor) exchangeName() string {
	return allowedSymbols.ReplaceAllString(c.Flow, "")
}
func (c *Monitor) createExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(c.exchangeName(), "topic", true, false, false, false, nil)
}

func (m *Monitor) createMonitorQueue(ch *amqp.Channel) (string, error) {
	q, err := ch.QueueDeclare("", false, true, false, false, nil)
	return q.Name, err
}

func (m *Monitor) createMonitor(ch *amqp.Channel) (string, error) {
	if err := m.createExchange(ch); err != nil {
		return "", errors.Wrap(err, "create exchange")
	}
	queue, err := m.createMonitorQueue(ch)
	if err != nil {
		return "", errors.Wrap(err, "create monitor queue")
	}

	for _, event := range m.Events {
		err = ch.QueueBind(queue, event, m.exchangeName(), false, nil)
		if err != nil {
			return "", errors.Wrap(err, "monitor queue to "+event)
		}
	}
	return queue, nil
}

func (m *Monitor) consumeMonitor(ctx context.Context, ch *amqp.Channel, monitoringQueue string, printer MonitorPrinter) error {
	stream, err := ch.Consume(monitoringQueue, "", true, true, false, false, nil)
	if err != nil {
		errors.Wrap(err, "open consume monitor stream")
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-stream:
			if !ok {
				return errors.New("stream closed")
			}
			var headers = make(map[string]string)
			for k, v := range msg.Headers {
				headers[k] = fmt.Sprint(v)
			}
			printer(msg.Timestamp, msg.RoutingKey, msg.MessageId, msg.Body, headers)
		}
	}
}

func (m *Monitor) runMonitorWithConnection(ctx context.Context, conn *amqp.Connection, printer MonitorPrinter) error {
	for {
		ch, err := conn.Channel()
		if err != nil {
			return errors.Wrap(err, "open monitor channel")
		}
		queue, err := m.createMonitor(ch)
		if err != nil {
			log.Println("failed create monitor infrastracture:", err)
			goto CLOSE
		}

		err = m.consumeMonitor(ctx, ch, queue, printer)
		if err != nil {
			log.Println("failed consume monitor:", err)
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

func (m *Monitor) RunMonitorWithReconnect(ctx context.Context, opener ConnectionOpener, printer MonitorPrinter) error {
	for {
		conn, err := opener.OpenConnection(ctx)

		if err != nil {
			return err
		}
		err = m.runMonitorWithConnection(ctx, conn, printer)
		if err != nil {
			log.Println("failed run monitor:", err)
		}
		conn.Close()
		select {
		case <-time.After(m.Retry):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
