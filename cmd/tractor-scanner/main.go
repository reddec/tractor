package main

import (
	"gopkg.in/alecthomas/kingpin.v2"
	_ "github.com/lib/pq"
	"log"
	"github.com/jmoiron/sqlx"
	"time"
	"github.com/reddec/tractor/cmd/tractor-scanner/dbo"
	"github.com/reddec/tractor"
	"github.com/streadway/amqp"
	"github.com/pkg/errors"
	"database/sql"
	"fmt"
	"strconv"
	"github.com/gin-gonic/gin/json"
)

var (
	dbURL             = kingpin.Flag("database", "PostgreSQL database connection string").Short('d').Envar("TRACTOR_SCANNER_DB").Default("user=postgres dbname=postgres password=postgres sslmode=disable host=localhost").String()
	brokerURL         = kingpin.Flag("broker", "AMQP broker URL").Short('b').Envar("TRACTOR_SCANNER_BROKER").Default("amqp://guest:guest@localhost").String()
	reconnectInterval = kingpin.Flag("reconnect-db", "Reconnect interval to database").Default("5s").Envar("TRACTOR_SCANNER_RECONNECT_DB").Duration()
	flow              = kingpin.Arg("flow", "Flow name to monitor").Required().Envar("TRACTOR_SCANNER_FLOW").String()
)

func main() {
	kingpin.Parse()

	for {
		err := run()
		if err != nil {
			log.Println("run loop stopped:", err)
		}
		log.Println("waiting", *reconnectInterval, "before restart")
		time.Sleep(*reconnectInterval)
	}

	log.Println("finished")
}

func run() error {
	db, flow := getDB()
	defer db.Close()

	db.SetMaxIdleConns(0)
	db.SetConnMaxLifetime(30 * time.Second)

	conn, ch, stream := getBroker()
	defer ch.Close()
	defer conn.Close()
	log.Println("consuming message")
	for msg := range stream {
		var headers = make(map[string]string)
		for k, v := range msg.Headers {
			headers[k] = fmt.Sprint(v)
		}

		retry, _ := strconv.Atoi(headers[tractor.RetryHeader])
		from := headers[tractor.ServiceFromHeader]
		last := headers[tractor.ServiceHeader]

		tx, err := db.Beginx()
		if err != nil {
			return errors.Wrap(err, "open transaction")
		}

		fromSrv, err := getOrCreateService(from, tx)
		if err != nil {
			tx.Rollback()
			return errors.Wrap(err, "get from service")
		}

		lastSrv, err := getOrCreateService(last, tx)
		if err != nil {
			tx.Rollback()
			return errors.Wrap(err, "get last service")
		}

		event, err := getOrCreateEvent(msg.RoutingKey, tx)
		if err != nil {
			tx.Rollback()
			return errors.Wrap(err, "get event")
		}

		res, err := json.Marshal(headers)
		if err != nil {
			tx.Rollback()
			panic("can't marshal headers: " + err.Error()) // Honestly, I don't when it will be
		}

		var record = dbo.History{
			Body:           msg.Body,
			CreatedAt:      time.Now(),
			EventID:        event.ID,
			EventMsgid:     msg.MessageId,
			EventTimestamp: msg.Timestamp,
			FlowID:         flow.ID,
			JSONHeaders:    string(res),
			Repeat:         retry,
			FromServiceID:  fromSrv.ID,
			LastServiceID:  lastSrv.ID,
		}

		err = record.Insert(tx)
		if err != nil {
			tx.Rollback()
			return errors.Wrap(err, "insert")
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			return errors.Wrap(err, "commit")
		}

		err = msg.Ack(false)
		if err != nil {
			log.Println("failed ack AMQP message:", err)
		}

		log.Println("save event", event.Name, "from message", msg.MessageId, "under #", record.ID)
	}
	return errors.New("message stream stopped")
}

func getOrCreateService(name string, tx *sqlx.Tx) (*dbo.Service, error) {
	srv, err := dbo.ServiceByName(tx, name)
	if err == sql.ErrNoRows {
		srv = &dbo.Service{Name: name, CreatedAt: time.Now()}
		return srv, srv.Insert(tx)
	}
	return srv, err
}

func getOrCreateEvent(name string, tx *sqlx.Tx) (*dbo.Event, error) {
	srv, err := dbo.EventByName(tx, name)
	if err == sql.ErrNoRows {
		srv = &dbo.Event{Name: name, CreatedAt: time.Now()}
		return srv, srv.Insert(tx)
	}
	return srv, err
}

func getDB() (*sqlx.DB, *dbo.Flow) {
	log.Println("connecting to database")
	var db *sqlx.DB
	var flow *dbo.Flow
	var err error
	for {
		db, err = sqlx.Connect("postgres", *dbURL)
		if err != nil {
			log.Println("failed connect to database:", err)
			goto RECONNECT_DB
		}

		_, err = db.Exec(string(dbo.MustAsset("schema.sql")))
		if err != nil {
			log.Println("failed execute intialization script:", err)
			goto RECONNECT_DB
		}

		flow, err = addFlow(db)
		if err != nil {
			log.Println("failed add flow:", err)
			goto RECONNECT_DB
		}

		break
	RECONNECT_DB:
		if db != nil {
			db.Close()
		}
		log.Println("waiting", *reconnectInterval, "before reconnect")
		time.Sleep(*reconnectInterval)
	}
	return db, flow
}

func addFlow(db *sqlx.DB) (*dbo.Flow, error) {
	tx, err := db.Beginx()
	if err != nil {
		return nil, errors.Wrap(err, "open transaction")
	}
	fl, err := dbo.FlowByName(tx, *flow)
	if err == sql.ErrNoRows {
		fl = &dbo.Flow{CreatedAt: time.Now(), Name: *flow}
		if err = fl.Save(db); err != nil {
			tx.Rollback()
			return nil, errors.Wrap(err, "failed add flow to DB")
		}
	} else if err != nil {
		tx.Rollback()
		return nil, errors.Wrap(err, "failed get flow")
	}
	return fl, tx.Commit()
}

func getBroker() (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery) {
	exchangeName := tractor.NormalizeName(*flow)
	queueName := exchangeName + "!" + "_scanner"

	log.Println("connecting to broker")
	var err error
	var conn *amqp.Connection
	var ch *amqp.Channel
	var stream <-chan amqp.Delivery
	for {
		conn, err = amqp.Dial(*brokerURL)
		if err != nil {
			log.Println("failed connect to broker:", err)
			goto RECONNECT_BROKER
		}
		ch, err = conn.Channel()
		if err != nil {
			log.Println("failed open channel in broker:", err)
			goto RECONNECT_BROKER
		}

		err = ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil)
		if err != nil {
			log.Println("failed create exchange:", err)
			goto RECONNECT_BROKER
		}

		_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
		if err != nil {
			log.Println("failed create queue:", err)
			goto RECONNECT_BROKER
		}

		err = ch.QueueBind(queueName, "#", exchangeName, false, nil)
		if err != nil {
			log.Println("failed bind queue:", err)
			goto RECONNECT_BROKER
		}

		stream, err = ch.Consume(queueName, "", false, false, false, false, nil)
		if err != nil {
			log.Println("failed open stream:", err)
			goto RECONNECT_BROKER
		}

		break

	RECONNECT_BROKER:
		if ch != nil {
			ch.Close()
		}
		if conn != nil {
			conn.Close()
		}
		log.Println("waiting", *reconnectInterval, "before reconnect")
		time.Sleep(*reconnectInterval)
	}
	return conn, ch, stream
}
