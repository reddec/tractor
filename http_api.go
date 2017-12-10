package tractor

import (
	"github.com/gin-gonic/gin"
	"github.com/streadway/amqp"
	"sync"
	"context"
	"net/http"
	"log"
	"time"
	"github.com/twinj/uuid"
	"errors"
)

func RunHTTPApi(flow, binding string, retry time.Duration, ctx context.Context, brokerUrl string) {
	router := gin.Default()

	var ch *amqp.Channel
	var lock sync.Mutex
	exName := NormalizeName(flow)
	sf := &SafeExchange{
		ExchangeType: "topic",
		ExchangeName: exName,
		Retry:        retry,
		Handler: func(ctx context.Context, cn *amqp.Channel) error {
			ec := make(chan *amqp.Error, 1)
			lock.Lock()
			cn.NotifyClose(ec)
			ch = cn
			lock.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-ec:
				return err
			}
		},
	}

	router.POST("/:event", func(gctx *gin.Context) {
		data, err := gctx.GetRawData()
		if err != nil {
			gctx.AbortWithError(http.StatusBadRequest, err)
			return
		}
		lock.Lock()
		if ch == nil {
			gctx.AbortWithError(http.StatusInternalServerError, errors.New("channel not opened yet"))
			return
		}
		err = ch.Publish(exName, gctx.Param("event"), false, false, amqp.Publishing{
			Body:      data,
			MessageId: uuid.NewV4().String(),
			Timestamp: time.Now(),
		})
		if err != nil {
			ch.Close()
		}
		lock.Unlock()
		if err != nil {
			gctx.AbortWithError(http.StatusInternalServerError, err)
			return
		}
		gctx.AbortWithStatus(http.StatusNoContent)
	})

	srv := &http.Server{
		Addr:    binding,
		Handler: router,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := srv.ListenAndServe()
		if err != nil {
			log.Print("HTTP API not started:", err)
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := sf.Run(ctx, brokerUrl)
		log.Println("failed establish connection to broker:", err)
	}()
	log.Println("HTTP API initialized")
	<-ctx.Done()

	done, closer := context.WithTimeout(context.Background(), 5*time.Second)
	defer closer()
	srv.Shutdown(done)
	wg.Wait()
}
