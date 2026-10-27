// Package config loads hookrelay settings from a JSON file and lets
// environment variables override them (secrets should only ever live in env).
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Duration is a time.Duration that reads and writes strings like "30s".
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Source configures one webhook producer, reachable at POST /hooks/{name}.
type Source struct {
	Secret            string   `json:"secret,omitempty"`
	SignatureHeader   string   `json:"signature_header"`
	SignaturePrefix   string   `json:"signature_prefix,omitempty"`
	TimestampHeader   string   `json:"timestamp_header,omitempty"`
	Tolerance         Duration `json:"tolerance,omitempty"`
	IdempotencyHeader string   `json:"idempotency_header,omitempty"`
	IdempotencyField  string   `json:"idempotency_field,omitempty"`
	TargetURL         string   `json:"target_url"`
}

// Store selects the persistence backend.
type Store struct {
	Type string `json:"type"` // "memory" or "file"
	Path string `json:"path,omitempty"`
}

// Dispatcher tunes delivery to targets.
type Dispatcher struct {
	Workers        int      `json:"workers"`
	PollInterval   Duration `json:"poll_interval"`
	RequestTimeout Duration `json:"request_timeout"`
	MaxAttempts    int      `json:"max_attempts"`
	BackoffBase    Duration `json:"backoff_base"`
	BackoffMax     Duration `json:"backoff_max"`
}

// Config is the whole service configuration.
type Config struct {
	Addr         string             `json:"addr"`
	LogLevel     string             `json:"log_level"`
	MaxBodyBytes int64              `json:"max_body_bytes"`
	AdminToken   string             `json:"-"` // env only: HOOKRELAY_ADMIN_TOKEN
	Store        Store              `json:"store"`
	Dispatcher   Dispatcher         `json:"dispatcher"`
	Sources      map[string]*Source `json:"sources"`
}

// Default returns the settings used for anything the file leaves out.
func Default() Config {
	return Config{
		Addr:         ":8080",
		LogLevel:     "info",
		MaxBodyBytes: 1 << 20,
		Store:        Store{Type: "memory"},
		Dispatcher: Dispatcher{
			Workers:        4,
			PollInterval:   Duration(time.Second),
			RequestTimeout: Duration(10 * time.Second),
			MaxAttempts:    8,
			BackoffBase:    Duration(time.Second),
			BackoffMax:     Duration(5 * time.Minute),
		},
	}
}

// Load reads path (skipped when empty), applies env overrides via getenv and
// validates the result.
func Load(path string, getenv func(string) string) (Config, error) {
	cfg := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields() // a typo in a key should fail loudly, not silently default
		if err := dec.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("parse %s: %w", path, err)
		}
	}
	if err := cfg.applyEnv(getenv); err != nil {
		return Config{}, err
	}
	return cfg, cfg.Validate()
}

func (c *Config) applyEnv(getenv func(string) string) error {
	set := func(key string, dst *string) {
		if v := getenv(key); v != "" {
			*dst = v
		}
	}
	setInt := func(key string, dst *int) error {
		if v := getenv(key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			*dst = n
		}
		return nil
	}
	set("HOOKRELAY_ADDR", &c.Addr)
	set("HOOKRELAY_LOG_LEVEL", &c.LogLevel)
	set("HOOKRELAY_ADMIN_TOKEN", &c.AdminToken)
	set("HOOKRELAY_STORE", &c.Store.Type)
	set("HOOKRELAY_STORE_PATH", &c.Store.Path)
	if err := setInt("HOOKRELAY_WORKERS", &c.Dispatcher.Workers); err != nil {
		return err
	}
	if err := setInt("HOOKRELAY_MAX_ATTEMPTS", &c.Dispatcher.MaxAttempts); err != nil {
		return err
	}
	for name, src := range c.Sources {
		if src == nil {
			continue
		}
		suffix := EnvSuffix(name)
		set("HOOKRELAY_SECRET_"+suffix, &src.Secret)
		set("HOOKRELAY_TARGET_"+suffix, &src.TargetURL)
	}
	return nil
}

// EnvSuffix maps a source name to its env var suffix: "my-shop" -> "MY_SHOP".
func EnvSuffix(source string) string {
	return strings.ToUpper(strings.ReplaceAll(source, "-", "_"))
}

var sourceName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// Validate reports every problem at once.
func (c Config) Validate() error {
	var errs []error
	bad := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if c.MaxBodyBytes <= 0 {
		bad("max_body_bytes must be > 0")
	}
	switch c.Store.Type {
	case "memory":
	case "file":
		if c.Store.Path == "" {
			bad("store.path is required for the file store")
		}
	default:
		bad("store.type must be \"memory\" or \"file\", got %q", c.Store.Type)
	}
	d := c.Dispatcher
	if d.Workers < 1 || d.MaxAttempts < 1 {
		bad("dispatcher.workers and dispatcher.max_attempts must be >= 1")
	}
	if d.PollInterval <= 0 || d.RequestTimeout <= 0 || d.BackoffBase <= 0 || d.BackoffMax < d.BackoffBase {
		bad("dispatcher durations must be > 0 and backoff_max >= backoff_base")
	}
	if len(c.Sources) == 0 {
		bad("at least one source is required")
	}
	for name, s := range c.Sources {
		if !sourceName.MatchString(name) {
			bad("source %q: name must match %s", name, sourceName)
		}
		if s == nil {
			bad("source %q: empty definition", name)
			continue
		}
		if s.Secret == "" {
			bad("source %q: secret is empty (set HOOKRELAY_SECRET_%s)", name, EnvSuffix(name))
		}
		if s.SignatureHeader == "" {
			bad("source %q: signature_header is required", name)
		}
		if s.Tolerance < 0 || (s.Tolerance > 0 && s.TimestampHeader == "") {
			bad("source %q: tolerance needs timestamp_header and must be >= 0", name)
		}
		if u, err := url.Parse(s.TargetURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			bad("source %q: target_url must be an absolute http(s) URL", name)
		}
	}
	return errors.Join(errs...)
}
