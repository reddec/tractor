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
type MonitorPrinterErr func(tm time.Time, event string, msgId string, msg []byte, headers map[string]string) error

type Monitor struct {
	Flow      string        `long:"name" description:"Flow name" default:"tractor-run" env:"FLOW_NAME"`
	Retry     time.Duration `long:"retry" description:"Channel initialize delay" default:"5s"`
	Events    []string      `long:"event" description:"Listening event" default:"#"`
	QueueName string        `long:"queue" description:"Monitoring queue" default:"monitor"`
}

func (c *Monitor) exchangeName() string {
	return allowedSymbols.ReplaceAllString(c.Flow, "")
}
func (c *Monitor) createExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(c.exchangeName(), "topic", true, false, false, false, nil)
}

func (m *Monitor) createMonitorQueue(ch *amqp.Channel) (string, error) {
	queueName := allowedSymbols.ReplaceAllString(m.QueueName, "")
	if queueName != "" {
		queueName = m.exchangeName() + "!" + queueName
	}
	q, err := ch.QueueDeclare(queueName, queueName != "", queueName == "", false, false, nil)
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
	stream, err := ch.Consume(monitoringQueue, "", true, false, false, false, nil)
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

func (m *Monitor) consumeMonitorWithError(ctx context.Context, ch *amqp.Channel, monitoringQueue string, printer MonitorPrinterErr) error {
	stream, err := ch.Consume(monitoringQueue, "", false, false, false, false, nil)
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
			err = printer(msg.Timestamp, msg.RoutingKey, msg.MessageId, msg.Body, headers)
			if err != nil {
				msg.Nack(false, true)
				return errors.Wrap(err, "process message")
			} else if err = msg.Ack(false); err != nil {
				return errors.Wrap(err, "commit messsage in broker")
			}
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

func (m *Monitor) runErrMonitorWithConnection(ctx context.Context, conn *amqp.Connection, printer MonitorPrinterErr) error {
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

		err = m.consumeMonitorWithError(ctx, ch, queue, printer)
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

func (m *Monitor) RunMonitorWithErrReconnect(ctx context.Context, opener ConnectionOpener, printer MonitorPrinterErr) error {
	for {
		conn, err := opener.OpenConnection(ctx)

		if err != nil {
			return err
		}
		err = m.runErrMonitorWithConnection(ctx, conn, printer)
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
