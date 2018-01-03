package utils

import (
	"github.com/jmoiron/sqlx"
	"time"
	"context"
	_ "github.com/lib/pq"
	"log"
	"math/rand"
)

const poolDriver = "postgres"

type DatabasePool struct {
	URLs             []string      `long:"database-url" short:"d" description:"Database connection string" default:"host=localhost user=postgres password=postgres sslmode=disable dbname=postgres" env:"DATABASE_URL"`
	ReconnectTimeout time.Duration `long:"db-reconnect-timeout" description:"Delay between connections attempts" default:"5s"`
	currentPool      *sqlx.DB
	offset           int
}

func DefaultDBPool(url ...string) *DatabasePool {
	return &DatabasePool{
		URLs:             url,
		ReconnectTimeout: 5 * time.Second,
		offset:           rand.Intn(len(url)),
	}
}

func (dp *DatabasePool) Close() error {
	if dp.currentPool != nil {
		return dp.currentPool.Close()
	}
	return nil
}

func (dp *DatabasePool) tryConnect() error {
	if dp.currentPool != nil {
		dp.currentPool.Close()
	}
	conn, err := sqlx.Connect(poolDriver, dp.URLs[dp.offset])
	if err == nil {
		dp.currentPool = conn
	}
	return err
}

func (dp *DatabasePool) TryOpenTransaction() (*sqlx.Tx, error) {
	if dp.currentPool == nil {
		err := dp.tryConnect()
		if err != nil {
			return nil, err
		}
	}
	return dp.currentPool.Beginx()
}

func (dp *DatabasePool) OpenTransaction(ctx context.Context) (*sqlx.Tx, error) {
	for {
		tx, err := dp.TryOpenTransaction()
		if err == nil {
			return tx, nil
		}
		log.Println("failed open transaction on", dp.URLs[dp.offset], ":", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(dp.ReconnectTimeout):
		}
		dp.offset = (dp.offset + 1) % len(dp.URLs)
	}
}
