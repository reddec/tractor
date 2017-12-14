package tractor

import (
	"github.com/streadway/amqp"
	"github.com/pkg/errors"
	"context"
	"log"
	"strconv"
	"time"
	"net"
	"fmt"
	"bytes"
)

const (
	RetryHeader       = "RETRY"
	LastRetryHeader   = "LAST_RETRY"
	ErrorHeader       = "LAST_ERROR"
	ServiceHeader     = "LAST_SERVICE"
	ServiceFromHeader = "FROM_SERVICE"
)

func (c *Config) queueName() string {
	return c.exchangeName() + "!" + allowedSymbols.ReplaceAllString(c.Name, "")
}

func (c *Config) exchangeName() string {
	return allowedSymbols.ReplaceAllString(c.Flow, "")
}

func (c *Config) requeueExchangeName() string {
	return allowedSymbols.ReplaceAllString(c.Flow, "") + "/requeue"
}

func (c *Config) requeueEventQueueNames() map[string]string {
	var ans = make(map[string]string)
	for _, lst := range c.Listen {
		ans[lst] = c.requeueExchangeName() + "!" + lst
	}
	return ans
}

func (c *Config) bind(ch *amqp.Channel, queue string) error {
	for _, listen := range c.Listen {
		err := ch.QueueBind(queue, listen, c.exchangeName(), false, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) createQueue(ch *amqp.Channel) error {
	_, err := ch.QueueDeclare(c.queueName(), true, false, false, false, nil)
	return err
}

func (c *Config) createRequeueQueues(ch *amqp.Channel) error {
	args := make(amqp.Table)
	args["x-dead-letter-exchange"] = c.exchangeName()
	for _, queue := range c.requeueEventQueueNames() {
		_, err := ch.QueueDeclare(queue, true, false, false, false, args)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) bindRequeueQueues(ch *amqp.Channel) error {
	for event, queue := range c.requeueEventQueueNames() {
		err := ch.QueueBind(queue, event, c.requeueExchangeName(), false, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) createExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(c.exchangeName(), "topic", true, false, false, false, nil)
}

func (c *Config) createRequeueExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(c.requeueExchangeName(), "direct", true, false, false, false, nil)
}

func (c *Config) Create(ch *amqp.Channel) error {
	if err := c.createExchange(ch); err != nil {
		return errors.Wrap(err, "create exchange")
	}
	if err := c.createQueue(ch); err != nil {
		return errors.Wrap(err, "create queue")
	}
	if err := c.createRequeueExchange(ch); err != nil {
		return errors.Wrap(err, "create requeue exchange")
	}
	if err := c.createRequeueQueues(ch); err != nil {
		return errors.Wrap(err, "create requeue-queues")
	}
	if err := c.bindRequeueQueues(ch); err != nil {
		return errors.Wrap(err, "bind requeues")
	}
	if err := c.bind(ch, c.queueName()); err != nil {
		return errors.Wrap(err, "bind queue")
	}
	return nil
}

func (c *Config) Consume(ctx context.Context, ch *amqp.Channel) error {
	stream, err := ch.Consume(c.queueName(), "", false, false, false, false, nil)
	if err != nil {
		errors.Wrap(err, "open consume stream")
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
			message, err := c.Run(msg.Body, msg.MessageId, msg.RoutingKey, headers, ctx)

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:

			}

			if msg.Headers == nil {
				msg.Headers = make(amqp.Table)
			}
			msg.Headers[ServiceHeader] = c.Name
			replyRequired := isCall(&msg)

			if err != nil {
				log.Println(c.Name, "failed:", err)
				err = c.runFailed(ch, msg, headers, err)
			} else if c.Event != "" || replyRequired {
				delete(msg.Headers, RetryHeader)
				msg.Headers[ServiceFromHeader] = c.Name
				var parts [][]byte

				if c.Multiple && !replyRequired {
					parts = bytes.Split(message, []byte("\n"))
				} else {
					parts = [][]byte{message}
				}

				for _, part := range parts {
					if len(part) == 0 {
						continue
					}

					pub := amqp.Publishing{
						MessageId: msg.MessageId,
						Body:      part,
						Headers:   msg.Headers,
						Timestamp: time.Now(),
					}

					if replyRequired {
						log.Println("reply to", msg.ReplyTo)
						pub.CorrelationId = msg.CorrelationId
						err = ch.Publish("", msg.ReplyTo, false, false, pub)
					} else {
						err = ch.Publish(c.exchangeName(), c.Event, false, false, pub)
					}

					if err != nil {
						break
					}
				}
				if err == nil {
					err = msg.Ack(false)
				}
			} else {
				// no next hop
				err = msg.Ack(false)
			}
			if err != nil {
				return err
			}
		}
	}
}

func (c *Config) runFailed(ch *amqp.Channel, msg amqp.Delivery, headers map[string]string, reason error) error {
	retries, _ := strconv.Atoi(headers[RetryHeader])
	msg.Headers[RetryHeader] = strconv.Itoa(retries + 1)
	msg.Headers[ErrorHeader] = reason.Error()
	msg.Headers[LastRetryHeader] = strconv.Itoa(retries)
	var err error
	// notify that service failed
	if c.FailEvent != "" {
		oldFrom := msg.Headers[ServiceFromHeader]
		oldRetry := msg.Headers[RetryHeader]
		msg.Headers[ServiceFromHeader] = c.Name
		delete(msg.Headers, RetryHeader)
		err = ch.Publish(c.exchangeName(), c.FailEvent, false, false, amqp.Publishing{
			MessageId: msg.MessageId,
			Body:      msg.Body,
			Headers:   msg.Headers,
			Timestamp: time.Now(),
		})
		if err != nil {
			return err
		}
		msg.Headers[ServiceFromHeader] = oldFrom
		msg.Headers[RetryHeader] = oldRetry
	}
	// requeue ?
	if c.Retry.Limit < 0 || retries < c.Retry.Limit {
		err = ch.Publish(c.requeueExchangeName(), msg.RoutingKey, false, false, amqp.Publishing{
			MessageId:     msg.MessageId,
			Body:          msg.Body,
			Headers:       msg.Headers,
			Expiration:    strconv.FormatInt(int64(c.Requeue/time.Millisecond), 10),
			Timestamp:     time.Now(),
			ReplyTo:       msg.ReplyTo,
			CorrelationId: msg.CorrelationId,
		})
	} else if c.Retry.ExceededEvent != "" {
		// notify that retries limit exceeded as normal event
		delete(msg.Headers, RetryHeader)
		msg.Headers[ServiceFromHeader] = c.Name
		err = ch.Publish(c.exchangeName(), c.Retry.ExceededEvent, false, false, amqp.Publishing{
			MessageId: msg.MessageId,
			Body:      msg.Body,
			Headers:   msg.Headers,
			Timestamp: time.Now(),
		})
	} else {
		log.Println(c.Name, msg.MessageId, "retries limit exceeded")
	}
	if err == nil {
		err = msg.Nack(false, false)
	}
	return err
}

func (c *Config) runWithConnection(ctx context.Context, conn *amqp.Connection) error {
	for {
		ch, err := conn.Channel()
		if err != nil {
			return errors.Wrap(err, "open channel for "+c.Name)
		}
		err = ch.Qos(1, 0, true)
		if err != nil {
			return errors.Wrap(err, "failed set prefetch size")
		}
		err = c.Create(ch)
		if err != nil {
			log.Println("failed create infrastracture:", err)
			goto CLOSE
		}

		err = c.Consume(ctx, ch)
		if err != nil {
			log.Println("failed consume:", err)
			goto CLOSE
		}

	CLOSE:
		ch.Close()
		select {
		case <-time.After(c.Reconnect):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

}

func (c *Config) RunWithReconnect(ctx context.Context, url string) error {
	for {
		conn, err := amqp.DialConfig(url, amqp.Config{
			Heartbeat: 10 * time.Second,
			Locale:    "en_US",
			Dial: func(network, addr string) (net.Conn, error) {
				var d net.Dialer
				d.Timeout = c.Connect
				return d.DialContext(ctx, network, addr)
			},
		})
		if err != nil {
			log.Println("failed connect:", err)
		} else {
			err = c.runWithConnection(ctx, conn)
			if err != nil {
				log.Println("failed run:", err)
			}
			conn.Close()
		}
		select {
		case <-time.After(c.Reconnect):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Config) RemoveQueues(ch *amqp.Channel) error {
	for _, q := range c.requeueEventQueueNames() {
		_, err := ch.QueueDelete(q, false, false, false)
		if err != nil {
			return errors.Wrap(err, "remove queue for requeue ("+q+")")
		}
	}
	_, err := ch.QueueDelete(c.queueName(), false, false, false)
	if err != nil {
		return errors.Wrap(err, "remove main queue")
	}
	return nil
}

func (c *Config) RemoveExchange(ch *amqp.Channel) error {
	return ch.ExchangeDelete(c.exchangeName(), false, false)
}

func (c *Config) RemoveRequeueExchange(ch *amqp.Channel) error {
	return ch.ExchangeDelete(c.requeueExchangeName(), false, false)
}
