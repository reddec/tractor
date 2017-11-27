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
)

type Config struct {
	Flow      string            `yaml:"-"`                // from directory
	Name      string            `yaml:"-"`                // from file name
	WorkDir   string            `yaml:"-"`                // from directory
	App       string            `yaml:"app"`              // required application executable (shell)
	Args      []string          `yaml:"args"`             // application arguments
	Event     string            `yaml:"event,omitempty"`  // generated event
	Listen    []string          `yaml:"listen,omitempty"` // on what events will be triggered
	Requeue   time.Duration     `yaml:"requeue"`          // Requeue delay
	Reconnect time.Duration     `yaml:"reconnect"`        // Reconnect (re-create channel or re-dial) timeout
	Connect   time.Duration     `yaml:"connect"`          // Connect timeout
	Env       map[string]string `yaml:"env"`              // additional environment
	Scale     int               `yaml:"scale"`            // how much instances has to be run
	Retry struct {
		Limit         int    `yaml:"limit"`          // Retries limit. By default - -1. Negative value means infinity
		ExceededEvent string `yaml:"exceeded_event"` // Event that will be emitted when no more retries left
	} `yaml:"retry"`
	FailEvent string `yaml:"fail_event"` // Event that will be emitted when application exited with non-zero code
}

var allowedSymbols = regexp.MustCompile(`[^a-zA-Z\-\._0-9 \$@]+`)

func (c *Config) Run(message []byte, messageId, event string, headers map[string]string, ctx context.Context) ([]byte, error) {
	cmd := exec.CommandContext(ctx, c.App, c.Args...)
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
	cmd.Env = append(cmd.Env, "MESSAGE_ID", messageId)
	cmd.Env = append(cmd.Env, "EVENT", event)

	cmd.Dir = c.WorkDir
	buffer := &bytes.Buffer{}
	cmd.Stdout = io.MultiWriter(os.Stderr, buffer)
	cmd.Stdin = bytes.NewBuffer(message)
	err := cmd.Run()
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), err
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
	var cfg Config
	// Default values
	cfg.Requeue = 10 * time.Second
	cfg.Reconnect = 15 * time.Second
	cfg.Connect = 30 * time.Second
	cfg.Retry.Limit = -1
	cfg.Scale = 1

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
