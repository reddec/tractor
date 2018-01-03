package utils

import (
	"time"
	"context"
	"github.com/streadway/amqp"
	"net"
	"github.com/pkg/errors"
	"log"
	"math/rand"
)

type BrokerPool struct {
	URLs             []string      `long:"broker-url" short:"b" description:"Broker connection string" env:"BROKER_URL" default:"amqp://guest:guest@localhost:5672/"`
	ConnectTimeout   time.Duration `long:"connect-timeout" description:"Connection timeout" default:"20s"`
	ReconnectTimeout time.Duration `long:"reconnect-timeout" description:"Delay between connections attempts" default:"5s"`
	offset           int
}

func (bp *BrokerPool) TryOpenConnection(ctx context.Context) (*amqp.Connection, error) {
	conn, err := amqp.DialConfig(bp.URLs[bp.offset], amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial: func(network, addr string) (net.Conn, error) {
			var d net.Dialer
			d.Timeout = bp.ConnectTimeout
			return d.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "connect to broker "+bp.URLs[bp.offset])
	}
	return conn, nil
}

func (bp *BrokerPool) OpenConnection(ctx context.Context) (*amqp.Connection, error) {
	for {
		conn, err := bp.TryOpenConnection(ctx)
		if err == nil {
			return conn, nil
		}
		log.Println("failed open broker connection:", err)
		select {
		case <-time.After(bp.ReconnectTimeout):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		bp.offset = (bp.offset + 1) % len(bp.URLs)
	}
}

func DefaultPool(url ...string) *BrokerPool {
	return &BrokerPool{
		URLs:             url,
		ConnectTimeout:   20 * time.Second,
		ReconnectTimeout: 3 * time.Second,
		offset:           rand.Intn(len(url)),
	}
}
