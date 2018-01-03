package main

import (
	"github.com/reddec/tractor/utils"
	"github.com/reddec/tractor/cmd/tractor-scanner/dbo"
	"github.com/jessevdk/go-flags"
	"os"
	"context"
	"os/signal"
	"syscall"
	"log"
	"github.com/pkg/errors"
	"github.com/reddec/tractor"
	"time"
	"database/sql"
	"github.com/lib/pq"
	"strconv"
	"fmt"
)

var VERSION string

var config struct {
	BrokerPool   utils.BrokerPool   `group:"Broker configuration"`
	DatabasePool utils.DatabasePool `group:"Database configuration"`
	Monitor      tractor.Monitor    `group:"Monitoring configuration"`
	ShowVersion  bool               `long:"version" description:"Show version and exit"`
}

func main() {

	_, err := flags.Parse(&config)

	if err != nil {
		return
	}

	if config.ShowVersion {
		fmt.Println(VERSION)
		return
	}

	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		closer()
	}()

	err = run(ctx)

	if err != nil {
		log.Println("finished with error:", err)
	}
}

func getHeader(headers map[string]string, key string) sql.NullString {
	if v, ok := headers[key]; ok {
		return sql.NullString{Valid: true, String: v}
	}
	return sql.NullString{}
}

func run(ctx context.Context) error {
	err := initDb(ctx)
	if err != nil {
		return errors.Wrap(err, "init db")
	}
	log.Println("DB initialized")
	return config.Monitor.RunMonitorWithErrReconnect(ctx, &config.BrokerPool, func(tm time.Time, event string, msgId string, msg []byte, headers map[string]string) error {
		log.Println("event:", event, "event-id:", msgId, "event-stamp:", tm)
		var evId sql.NullString
		if msgId != "" {
			evId.String = msgId
			evId.Valid = true
		}
		var retries sql.NullInt64
		if v, ok := headers[tractor.RetryHeader]; ok {
			iv, err := strconv.ParseInt(v, 10, 64)
			retries.Valid = err == nil
			retries.Int64 = iv

		}
		var lastRetries sql.NullInt64
		if v, ok := headers[tractor.LastRetryHeader]; ok {
			iv, err := strconv.ParseInt(v, 10, 64)
			lastRetries.Valid = err == nil
			lastRetries.Int64 = iv

		}
		dbEvent := dbo.TractorEvent{
			Event:       event,
			EventID:     evId,
			Payload:     msg,
			Flow:        config.Monitor.Flow,
			CreatedAt:   time.Now(),
			Source:      getHeader(headers, tractor.ServiceFromHeader),
			LastService: getHeader(headers, tractor.ServiceHeader),
			FromService: getHeader(headers, tractor.ServiceFromHeader),
			LastError:   getHeader(headers, tractor.ErrorHeader),
			EventStamp:  pq.NullTime{Valid: true, Time: tm},
			Retry:       retries,
			LastRetry:   lastRetries,
		}
		tx, err := config.DatabasePool.OpenTransaction(ctx)
		if err != nil {
			return err
		}
		err = dbEvent.Insert(tx)
		if err != nil {
			return tx.Rollback()
		}
		return tx.Commit()
	})
}

func initDb(ctx context.Context) error {
	log.Println("preparing db")
	tx, err := config.DatabasePool.OpenTransaction(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(string(dbo.MustAsset("schema.sql")))
	log.Println("commiting changes")
	if err != nil {
		return tx.Rollback()
	}
	return tx.Commit()
}
