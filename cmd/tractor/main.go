package main

import (
	"gopkg.in/alecthomas/kingpin.v2"
	"context"
	"github.com/reddec/tractor"
	"io/ioutil"
	"log"
	"strings"
	"path/filepath"
	"sync"
	"os"
	"os/signal"
	"syscall"
	"fmt"
	"strconv"
	"github.com/streadway/amqp"
	"github.com/twinj/uuid"
	"github.com/fatih/color"
	"time"
	"gopkg.in/yaml.v2"
	"bufio"
)

var (
	brokerUrl = kingpin.Flag("broker", "Broker AMQP URL").Short('b').Default("amqp://guest:guest@localhost:5672").Envar("TRACTOR_BROKER").String()
	from      = kingpin.Flag("from", "Path to directory with configurations files").Short('f').Default(".").Envar("TRACTOR_FROM").String()
)

var (
	sampleCmd      = kingpin.Command("sample", "Make sample config for provided command")
	sampleArgs     = sampleCmd.Arg("exec", "Exec and args").Required().Strings()
	sampleListen   = sampleCmd.Flag("listen", "Listen events").Short('l').Strings()
	sampleEvent    = sampleCmd.Flag("event", "Provide event").Short('e').String()
	sampleEnv      = sampleCmd.Flag("env", "Custom env").Short('E').StringMap()
	sampleMultiple = sampleCmd.Flag("multiple", "Mark output as multiple messages separated by \\n").Short('m').Bool()
)

var (
	runCmd           = kingpin.Command("run", "Run single instance of service in flow")
	runArgs          = runCmd.Arg("exec", "Exec and args").Required().Strings()
	runListen        = runCmd.Flag("listen", "Listen events").Short('l').Strings()
	runEvent         = runCmd.Flag("event", "Provide event").Short('e').String()
	runMultiple      = runCmd.Flag("multiple", "Mark output as multiple messages separated by \\n").Short('m').Bool()
	runFlow          = runCmd.Flag("flow", "Flow name").Short('F').Default("tractor-run").Envar("FLOW_NAME").String()
	runName          = runCmd.Flag("name", "Application name").Short('n').Envar("APP_NAME").Required().String()
	runStream        = runCmd.Flag("stream", "Always start this application, no listen, only provides, each line is event, each line - new MessageID").Short('s').Bool()
	runRetries       = runCmd.Flag("retries", "Maximum count of retries").Short('r').Default("-1").Int()
	runLimitExecTime = runCmd.Flag("limit-exec-time", "Limit maximum execution time").Envar("LIMIT_EXEC_TIME").Duration()
	runGracefulTime  = runCmd.Flag("limit-graceful-time", "Limit maximum graceful delay before termination").Default("3s").Envar("LIMIT_GRACEFUL_TIME").Duration()
)

var (
	callCmd         = kingpin.Command("call", "Send event and wait for reply")
	callFlow        = callCmd.Flag("flow", "Flow name").Short('F').Default("tractor-run").Envar("FLOW_NAME").String()
	callEvent       = callCmd.Arg("exec", "Event/method name").Envar("EVENT").Required().String()
	callTimeout     = callCmd.Flag("timeout", "Call timeout").Short('t').Envar("TIMEOUT").Default("30s").Duration()
	callInteractive = callCmd.Flag("interactive", "Read line as single message").Short('i').Envar("INTERACTIVE").Bool()
)

var (
	startCmd        = kingpin.Command("start", "Start flow")
	startHttpServer = kingpin.Flag("http", "HTTP publish binding").Default("127.0.0.1:5040").String()
)
var (
	rmCmd = kingpin.Command("rm", "Remove flow infrastructure")
)
var (
	monitorCmd    = kingpin.Command("monitor", "Monitor events for services")
	monitorFull   = monitorCmd.Flag("full", "Dump full content of event").Bool()
	monitorEvents = monitorCmd.Arg("event", "Filter events. If empty - all").Strings()
	monitorBW     = monitorCmd.Flag("pure", "Disable colored output").Bool()
)

var (
	eventCmd = kingpin.Command("event", "Events operations")
)
var (
	listEventCmd    = eventCmd.Command("ls", "List events described in flow")
	listEventListen = listEventCmd.Flag("listen", "Show only events that are listening").Short('l').Bool()
)

var (
	pushEventCmd  = eventCmd.Command("push", "Push event (from stdin) to flow")
	pushEventName = pushEventCmd.Arg("event", "Event name").Required().String()
	pushEnv       = eventCmd.Flag("env", "Additional meta-header for event").Short('e').StringMap()
)

func main() {
	log.SetPrefix("[tractor] ")
	log.SetOutput(os.Stderr)
	v := kingpin.Parse()
	switch  v {
	case startCmd.FullCommand():
		start()
	case rmCmd.FullCommand():
		rm()
	case listEventCmd.FullCommand():
		listEvent()
	case pushEventCmd.FullCommand():
		pushEvent()
	case monitorCmd.FullCommand():
		monitor()
	case sampleCmd.FullCommand():
		sample()
	case runCmd.FullCommand():
		run()
	case callCmd.FullCommand():
		call()
	default:
		log.Println("unknown command:", v)
	}
}

func call() {
	var data []byte
	var err error
	if !*callInteractive {
		data, err = ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal("read:", err)
		}
	}

	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	session, err := tractor.BindSession(*brokerUrl, *callFlow, *callEvent, *callTimeout, ctx)
	if err != nil {
		log.Fatal("failed create session: ", err)
	}
	defer session.Close()

	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		closer()
		os.Stdin.Close()
	}()

	if *callInteractive {
		reader := bufio.NewScanner(os.Stdin)
		for reader.Scan() {
			data = reader.Bytes()
			data, err = session.Invoke(data, *callTimeout, ctx)
			if err != nil {
				log.Fatal("failed wait for reply:", err)
			}
			os.Stdout.Write(data)
		}
	} else {
		data, err = session.Invoke(data, *callTimeout, ctx)
		if err != nil {
			log.Fatal("failed wait for reply:", err)
		}
		os.Stdout.Write(data)
	}

}

func run() {
	var cfg = tractor.DefaultConfig()
	cfg.App = (*runArgs)[0]
	cfg.Args = (*runArgs)[1:]
	cfg.Listen = (*runListen)
	cfg.Event = *runEvent
	cfg.Multiple = *runMultiple
	cfg.Flow = *runFlow
	cfg.Stream = *runStream
	cfg.Name = *runName
	cfg.Retry.Limit = *runRetries
	cfg.Limits.ExecutionTime = *runLimitExecTime
	cfg.Limits.GracefulTime = *runGracefulTime

	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if cfg.Stream {
			log.Println("stream", cfg.Name, "started")
			err := cfg.RunStreamPublisher(ctx, *brokerUrl)
			log.Println("stream", cfg.Name, "finished. reason:", err)
		} else {
			log.Println("service", cfg.Name, "started")
			err := cfg.RunWithReconnect(ctx, *brokerUrl)
			log.Println("service", cfg.Name, "finished. reason:", err)
		}
	}()
	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		closer()
	}()

	<-done
}

func sample() {
	var cfg = tractor.DefaultConfig()
	cfg.App = (*sampleArgs)[0]
	cfg.Args = (*sampleArgs)[1:]
	cfg.Listen = (*sampleListen)
	cfg.Event = *sampleEvent
	cfg.Multiple = *sampleMultiple
	cfg.Env = *sampleEnv
	data, err := yaml.Marshal(cfg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(data))
}

func monitor() {
	var configs = getConfigs()

	var events = make(map[string][]string)

	for _, cfg := range configs {
		if cfg.Event != "" {
			events[cfg.Event] = events[cfg.Event]
		}
		for _, eve := range cfg.Listen {
			events[eve] = append(events[eve], cfg.Name)
		}
		if cfg.FailEvent != "" {
			events[cfg.FailEvent] = events[cfg.FailEvent]
		}
		if cfg.Retry.ExceededEvent != "" {
			events [cfg.Retry.ExceededEvent] = events[cfg.Retry.ExceededEvent]
		}
	}
	if len(*monitorEvents) != 0 {
		interest := makeSet(*monitorEvents)
		ns := make(map[string][]string)
		for eve, to := range events {
			if interest[eve] {
				ns[eve] = to
			}

		}
		events = ns
	}

	var (
		mxEvent   = 5
		mxFlow    = 4
		mxService = 7
		mxTo      = 2
	)
	toList := make(map[string]string)
	for eve, to := range events {
		if len(eve) > mxEvent {
			mxEvent = len(eve)
		}
		toS := strings.Join(unique(to), ",")
		if len(toS) > mxTo {
			mxTo = len(toS)
		}
		toList[eve] = toS
	}

	for _, cfg := range configs {
		if len(cfg.Flow) > mxFlow {
			mxFlow = len(cfg.Flow)
		}
		if len(cfg.Name) > mxService {
			mxService = len(cfg.Name)
		}
	}

	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	tmFmt := "% 30s"
	idFmt := "% 37s"
	sizeFmt := "% 6v"
	retryFmt := "% 6v"
	fmtStr := tmFmt + "  " + idFmt + "  %" + strconv.Itoa(mxEvent) + "s  %" + strconv.Itoa(mxService) + "s  %" + strconv.Itoa(mxService) + "s  %" + strconv.Itoa(mxTo) + "s  " + sizeFmt + "  " + retryFmt + "\n"
	if !*monitorFull {
		fmt.Printf(fmtStr, "TIME", "ID", "EVENT", "LAST", "FROM", "TO", "SIZE", "RETRY")
	}

	var eventsList []string
	for eve := range events {
		eventsList = append(eventsList, eve)
	}

	lock := sync.Mutex{}
	mon := tractor.Monitor{
		Retry:  10 * time.Second,
		Flow:   configs[0].Flow,
		Events: eventsList,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		mon.RunMonitorWithReconnect(ctx, *brokerUrl, func(tm time.Time, event string, id string, body []byte, headers map[string]string) {
			lock.Lock()
			defer lock.Unlock()

			if *monitorFull {
				fmt.Println("Flow:", mon.Flow)
				fmt.Println("Event:", event)
				fmt.Println("Time:", tm)
				fmt.Println("Id:", id)
				fmt.Println("Size:", len(body))
				fmt.Println("To:", toList[event])
				for k, v := range headers {
					fmt.Println(k+":", v)
				}
				fmt.Println("")
				fmt.Println(string(body))
				fmt.Println("")
			} else {
				retry, _ := strconv.Atoi(headers[tractor.RetryHeader])
				from := headers[tractor.ServiceFromHeader]
				last := headers[tractor.ServiceHeader]
				txt := fmt.Sprintf(fmtStr, tm, id, event, last, from, toList[event], len(body), retry)
				if !*monitorBW {
					c := color.New()
					if toList[event] == "" {
						c = c.Add(color.BgBlue)
					}
					if retry > 0 {
						c = c.Add(color.FgYellow)
					}
					txt = c.Sprint(txt)
				}
				fmt.Print(txt)
			}
		})
	}()

	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		closer()
	}()

	<-done
	log.Println("finished")
}

func rm() {
	var configs = getConfigs()

	conn, err := amqp.Dial(*brokerUrl)
	if err != nil {
		log.Fatal("failed connect:", err)

	}
	var failed = false
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		log.Fatal("failed open channel:", err)
	}
	for _, cfg := range configs {
		err = cfg.RemoveQueues(ch)
		if err != nil {
			failed = true
			log.Println("failed remove queue from", cfg.Name, ":", err)
		}
	}
	for _, cfg := range configs {
		err = cfg.RemoveRequeueExchange(ch)
		if err != nil {
			failed = true
			log.Println("failed remove requeue exchange from", cfg.Name, ":", err)
		}
	}
	for _, cfg := range configs {
		err = cfg.RemoveExchange(ch)
		if err != nil {
			failed = true
			log.Println("failed remove exchange from", cfg.Name, ":", err)
		}
	}
	ch.Close()
	conn.Close()
	if failed {
		log.Fatal("removed with errors")
	}
	log.Println("removed")
}

func getConfigs() []*tractor.Config {
	var configs []*tractor.Config

	items, err := ioutil.ReadDir(*from)
	if err != nil {
		log.Fatal("failed read dir:", err)
	}

	for _, item := range items {
		lName := strings.ToLower(item.Name())
		if strings.HasSuffix(lName, ".yaml") || strings.HasSuffix(lName, ".yml") {
			cfg, err := tractor.LoadConfig(filepath.Join(*from, item.Name()), true)
			if err != nil {
				log.Fatal("failed load config", lName, "-", err)
			}
			configs = append(configs, cfg)
		}
	}
	return configs
}

func pushEvent() {
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {

		log.Fatal("failed read stdin:", err)
	}

	conn, err := amqp.Dial(*brokerUrl)
	if err != nil {
		log.Fatal("failed connect:", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		log.Fatal("failed open channel:", err)
	}
	msgId := uuid.NewV4().String()
	flow := tractor.GetFlowFromDir(*from)

	var header = make(amqp.Table)

	for k, v := range *pushEnv {
		header[k] = v
	}

	err = ch.Publish(flow, *pushEventName, true, false, amqp.Publishing{
		MessageId: msgId,
		Headers:   header,
		Timestamp: time.Now(),
		Body:      data,
	})
	ch.Close()
	conn.Close()
	if err != nil {
		log.Fatal("failed send:", err)
	}
	fmt.Println(flow, msgId)
}

func listEvent() {
	items, err := ioutil.ReadDir(*from)
	if err != nil {
		log.Fatal("failed read dir:", err)
	}

	var unique = make(map[string]struct {
		listen  []string
		provide []string
	})

	for _, item := range items {
		lName := strings.ToLower(item.Name())
		if strings.HasSuffix(lName, ".yaml") || strings.HasSuffix(lName, ".yml") {
			cfg, err := tractor.LoadConfig(filepath.Join(*from, item.Name()), false)
			if err != nil {
				log.Fatal("failed load config", lName, "-", err)
			}
			if !*listEventListen {

				if cfg.Event != "" {
					eve := unique[cfg.Event]
					eve.provide = append(eve.provide, cfg.Name)
					unique[cfg.Event] = eve
				}

				if cfg.FailEvent != "" {
					eve := unique[cfg.FailEvent]
					eve.provide = append(eve.provide, cfg.Name)
					unique[cfg.FailEvent] = eve
				}
				if cfg.Retry.ExceededEvent != "" {
					eve := unique[cfg.Retry.ExceededEvent]
					eve.provide = append(eve.provide, cfg.Name)
					unique[cfg.Retry.ExceededEvent] = eve
				}

			}
			for _, event := range cfg.Listen {
				eve := unique[event]
				eve.listen = append(eve.listen, cfg.Name)
				unique[event] = eve
			}
		}
	}
	var (
		mxEvent   = 5
		mxListen  = 6
		mxProvide = 7
	)

	var rows = make([][]string, 0, len(unique))

	for event, descr := range unique {
		if len(event) > mxEvent {
			mxEvent = len(event)
		}
		listen := strings.Join(descr.listen, ",")
		if len(listen) > mxListen {
			mxListen = len(listen)
		}
		provide := strings.Join(descr.provide, ",")
		if len(provide) > mxProvide {
			mxProvide = len(provide)
		}
		rows = append(rows, []string{event, listen, provide})
	}
	fmtStr := "% " + strconv.Itoa(mxEvent) + "s    %" + strconv.Itoa(mxListen) + "s    %" + strconv.Itoa(mxProvide) + "s\n"
	fmt.Printf(fmtStr, "EVENT", "LISTEN", "PROVIDE")
	for _, row := range rows {
		fmt.Printf(fmtStr, row[0], row[1], row[2])
	}

}

func start() {
	var configs = getConfigs()

	if len(configs) == 0 {
		log.Fatal("nothing to start")
	}

	wg := sync.WaitGroup{}
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	for _, cfg := range configs {
		for i := 0; i < cfg.Scale; i++ {
			wg.Add(1)
			go func(i int, cfg *tractor.Config) {
				defer wg.Done()
				if cfg.Stream {
					log.Println("stream #", i, cfg.Name, "started")
					err := cfg.RunStreamPublisher(ctx, *brokerUrl)
					log.Println("stream #", i, cfg.Name, "finished. reason:", err)
				} else {
					log.Println("service", i, cfg.Name, "started")
					err := cfg.RunWithReconnect(ctx, *brokerUrl)
					log.Println("service", i, cfg.Name, "finished. reason:", err)
				}
			}(i, cfg)
		}
	}

	if *startHttpServer != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tractor.RunHTTPApi(configs[0].Flow, *startHttpServer, configs[0].Reconnect, ctx, *brokerUrl)
		}()
	}

	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		closer()
	}()

	wg.Wait()
	log.Println("finished")
}

func unique(names []string) []string {
	var hash = make(map[string]struct{})
	for _, name := range names {
		hash[name] = struct{}{}
	}
	var ans = make([]string, 0, len(hash))
	for name := range hash {
		ans = append(ans, name)
	}
	return ans
}

func makeSet(names []string) map[string]bool {
	var hash = make(map[string]bool)
	for _, name := range names {
		hash[name] = true
	}
	return hash
}
