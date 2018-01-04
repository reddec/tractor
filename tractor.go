package tractor

import (
	"regexp"
	"os/exec"
	"context"
	"io"
	"bytes"
	"os"
	"time"

	"path/filepath"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/pkg/errors"
	"strings"
	"bufio"
	"log"
	"github.com/reddec/tractor/dbo"
	"database/sql"
	"encoding/json"
	"github.com/reddec/tractor/utils"
)

type Config struct {
	Flow      string            `yaml:"-"`                  // from directory
	Name      string            `yaml:"-"`                  // from file name
	WorkDir   string            `yaml:"-"`                  // from directory
	App       string            `yaml:"app"`                // required application executable (shell)
	Args      []string          `yaml:"args,omitempty"`     // application arguments
	Event     string            `yaml:"event,omitempty"`    // generated event
	Multiple  bool              `yaml:"multiple,omitempty"` // each non-empty line from output will be used as separate event. Each line - same MessageID
	Stream    bool              `yaml:"stream,omitempty"`   // always start this application, no listen, only provides, each line is event, each line - new MessageID
	Listen    []string          `yaml:"listen,omitempty"`   // on what events will be triggered
	Requeue   time.Duration     `yaml:"requeue"`            // Requeue delay
	Reconnect time.Duration     `yaml:"reconnect"`          // Reconnect (re-create channel or re-dial) timeout
	Connect   time.Duration     `yaml:"connect"`            // Connect timeout
	Env       map[string]string `yaml:"env,omitempty"`      // additional environment
	Limits struct {
		ExecutionTime time.Duration `yaml:"execution_time,omitempty"` // Maximum execution time
		GracefulTime  time.Duration `yaml:"graceful_time,omitempty"`  // Graceful delay before hard kill of process
	} `yaml:"limits,omitempty"`
	Scale int `yaml:"scale"` // how much instances has to be run
	Retry struct {
		Limit         int    `yaml:"limit"`                    // Retries limit. By default - -1. Negative value means infinity
		ExceededEvent string `yaml:"exceeded_event,omitempty"` // Event that will be emitted when no more retries left
	} `yaml:"retry"`
	FailEvent string `yaml:"fail_event,omitempty"` // Event that will be emitted when application exited with non-zero code

	db *utils.DatabasePool // Save results to DB
}

func (c *Config) DB() *utils.DatabasePool { return c.db }

var allowedSymbols = regexp.MustCompile(`[^a-zA-Z\-\._0-9 \$@]+`)

func NormalizeName(name string) string {
	return allowedSymbols.ReplaceAllString(name, "")
}

func (c *Config) Run(message []byte, messageId, parentMessageId, event string, headers map[string]string, ctx context.Context) ([]byte, error) {
	startedAt := time.Now()
	app := c.App

	if c.Limits.ExecutionTime != 0 {
		tctx, cls := context.WithTimeout(ctx, c.Limits.ExecutionTime)
		defer cls()
		ctx = tctx
	}

	if strings.HasPrefix(app, "."+string(filepath.Separator)) || strings.HasPrefix(app, ".."+string(filepath.Separator)) {
		abs, err := filepath.Abs(filepath.Join(c.WorkDir, app))
		if err != nil {
			return nil, errors.Wrap(err, "get abs path to executable")
		}
		app = abs
	}

	cmd := exec.Command(app, c.Args...)
	setupCmdFlags(cmd)
	for k, v := range c.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	// copy current env
	for _, e := range os.Environ() {
		cmd.Env = append(cmd.Env, e)
	}
	// copy from headers
	for k, v := range headers {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	// add predefined
	cmd.Env = append(cmd.Env, "MESSAGE_ID="+messageId)
	cmd.Env = append(cmd.Env, "EVENT="+event)

	cmd.Dir = c.WorkDir
	buffer := &bytes.Buffer{}

	logOutIn, logOutOut := io.Pipe()
	defer logOutIn.Close()
	defer logOutOut.Close()

	logErrIn, logErrOut := io.Pipe()
	defer logErrIn.Close()
	defer logErrOut.Close()

	logStream(c.Name+"-stdout", logOutIn)
	logStream(c.Name+"-stderr", logErrIn)

	cmd.Stdout = io.MultiWriter(logOutOut, buffer)
	cmd.Stdin = bytes.NewBuffer(message)
	cmd.Stderr = logErrOut

	err := runWithContext(cmd, ctx, 2*time.Second)

	finishedAt := time.Now()

	if dbErr := c.saveResult(startedAt, finishedAt, message, buffer.Bytes(), messageId, parentMessageId, event, headers, err, ctx); dbErr != nil {
		return nil, dbErr
	}

	if err != nil {
		return nil, errors.Wrap(err, buffer.String())
	}
	return buffer.Bytes(), err
}

func (c *Config) saveResult(start, stop time.Time, message, result []byte, messageId, parentMessageId, event string, headers map[string]string, resErr error, ctx context.Context) error {
	if c.db == nil {
		return nil
	}
	var dbErr sql.NullString
	if resErr != nil {
		dbErr.Valid = true
		dbErr.String = resErr.Error()
	}

	headersData, _ := json.Marshal(headers)
	res := dbo.TractorResult{
		Event:         event,
		EventID:       messageId,
		ParentEventID: sql.NullString{Valid: parentMessageId != "", String: parentMessageId},
		FinishedAt:    stop,
		StartedAt:     start,
		Input:         message,
		Output:        result,
		JSONHeaders:   string(headersData),
		Err:           dbErr,
	}

	for {
		tx, err := c.db.OpenTransaction(ctx)
		if err != nil {
			return err
		}
		err = res.Insert(tx)
		if err != nil {
			log.Println("failed save result to DB:", err)
			goto WAIT
		} else {
			err = tx.Commit()
		}
		if err != nil {
			log.Println("failed commit result in DB:", err)
			goto WAIT
		}
		return nil
	WAIT:
		err = tx.Rollback()
		if err != nil {
			log.Println("failed rollback:", err)
		}
		select {
		case <-time.After(c.db.ReconnectTimeout):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Config) RunStream(ctx context.Context) (ch <-chan string) {
	var output = make(chan string)
	go func() {
		defer close(output)
		app := c.App
		if strings.HasPrefix(app, "."+string(filepath.Separator)) || strings.HasPrefix(app, ".."+string(filepath.Separator)) {
			abs, err := filepath.Abs(filepath.Join(c.WorkDir, app))
			if err != nil {
				log.Println(c.Name, "get abs path to executable error:", err)
				return
			}
			app = abs
		}

		cmd := exec.CommandContext(ctx, app, c.Args...)
		for k, v := range c.Env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		// copy current env
		for _, e := range os.Environ() {
			cmd.Env = append(cmd.Env, e)
		}

		cmd.Dir = c.WorkDir

		logOutIn, logOutOut := io.Pipe()
		defer logOutIn.Close()
		defer logOutOut.Close()

		logErrIn, logErrOut := io.Pipe()
		defer logErrIn.Close()
		defer logErrOut.Close()

		streamIn, streamOut := io.Pipe()

		logStream(c.Name+"-stdout", logOutIn)
		logStream(c.Name+"-stderr", logErrIn)

		cmd.Stdout = io.MultiWriter(logOutOut, streamOut)
		cmd.Stderr = logErrOut
		cmd.Stdin = os.Stdin // prevent apps close due to EOF

		done := make(chan struct{})

		go func() {
			defer close(done)
			bufr := bufio.NewReader(streamIn)
			for {
				data, err := bufr.ReadString('\n')
				if len(data) != 0 {
					select {
					case output <- data:
					case <-ctx.Done():
						return
					}
				}
				if err != nil {
					log.Println("stream", c.Name, "failed read:", err)
					break
				}
				select {
				case <-ctx.Done():
					return
				default:

				}
			}
		}()

		err := cmd.Run()
		streamOut.Close()
		<-done
		if err != nil {
			log.Println(c.Name, "stream finished with error:", err)
		} else {
			log.Println("stream", c.Name, "stopped")
		}
	}()
	return output
}

func (c *Config) Validate() error {
	var errs []error

	if c.Flow == "" {
		errs = append(errs, errors.New("flow name is empty"))
	}
	if c.Name == "" {
		errs = append(errs, errors.New("name is empty"))
	}
	if c.WorkDir == "" {
		errs = append(errs, errors.New("workdir is empty"))
	}
	if c.App == "" {
		errs = append(errs, errors.New("application (app) not defined"))
	}
	if c.Scale <= 0 {
		errs = append(errs, errors.New("invalid scale factor: must be more or equal 1"))
	}
	if len(errs) == 0 {
		return nil
	}
	t := ""
	for _, s := range errs {
		t += s.Error() + "\n"
	}
	return errors.New(t)
}

func GetFlowFromDir(dirName string) string {
	abs, _ := filepath.Abs(dirName)
	return allowedSymbols.ReplaceAllString(filepath.Base(abs), "")
}

func DefaultConfig() Config {
	var cfg Config
	// Default values
	cfg.Requeue = 10 * time.Second
	cfg.Reconnect = 15 * time.Second
	cfg.Connect = 30 * time.Second
	cfg.Retry.Limit = -1
	cfg.Scale = 1
	cfg.Limits.GracefulTime = 2 * time.Second
	return cfg
}

func DefaultConfigWithDB(db *utils.DatabasePool) Config {
	cfg := DefaultConfig()
	cfg.db = db
	return cfg
}

func LoadConfigWithDb(file string, validate bool, db *utils.DatabasePool) (*Config, error) {
	cfg, err := LoadConfig(file, validate)
	if err == nil {
		cfg.db = db
	}
	return cfg, err
}

func LoadConfig(file string, validate bool) (*Config, error) {
	if file == "" {
		return nil, errors.New("empty filename")
	}
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errors.Wrap(err, "read config "+file)
	}
	file, err = filepath.Abs(file)
	if err != nil {
		// really, I don't know when it will be
		return nil, errors.Wrap(err, "get asb path")
	}
	var cfg = DefaultConfig()

	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, errors.Wrap(err, "parse config "+file)
	}
	dirname := filepath.Base(filepath.Dir(file))
	cfg.WorkDir = filepath.Dir(file)
	cfg.Flow = dirname

	name := filepath.Base(file)
	dot := strings.LastIndex(name, ".")
	if dot > 0 {
		name = name[:dot]
	}
	cfg.Name = name

	if validate {
		err = cfg.Validate()
	}
	return &cfg, err
}

func logStream(prefix string, stream io.Reader) {
	go func() {
		bufr := bufio.NewReader(stream)
		for {
			data, err := bufr.ReadString('\n')
			if len(data) > 0 && data[len(data)-1] == '\n' {
				data = data[:len(data)-1]
			}
			if err != nil {
				if len(data) != 0 {
					log.Println("["+prefix+"]", data)
				}
				break
			}
			log.Println("["+prefix+"]", data)
		}
	}()
}
