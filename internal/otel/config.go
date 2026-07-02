package otel

import (
	"errors"
	"time"
)

// Config controls the OTLP/gRPC exporter. When enabled, the exporter consumes
// blocks from the same queue the insert workers use and exports them as OTLP to
// an OpenTelemetry endpoint instead of inserting into ClickHouse. Only one sink
// (insert or otel) can consume the queue at a time.
type Config struct {
	Enabled bool `yaml:"enabled"`

	// URL is the OTLP/gRPC endpoint, e.g. "localhost:4317". A leading
	// "http://" / "https://" / "grpc://" scheme is accepted and stripped.
	URL string `yaml:"url"`

	// Insecure disables transport security (plaintext gRPC). When false, TLS is
	// used. A "http://" scheme in URL also implies insecure.
	Insecure bool `yaml:"insecure"`

	// Threads is the number of concurrent exporter workers. Each worker holds one
	// gRPC connection and consumes blocks independently.
	Threads int `yaml:"threads"`

	// BatchSize is the number of rows accumulated (across one or more blocks)
	// before an export request is flushed. Larger batches mean fewer, larger RPCs.
	BatchSize int `yaml:"batch_size"`

	// FlushInterval bounds how long a partially-filled batch waits before being
	// flushed, so low-throughput sources still export promptly. Defaults to 1s.
	FlushInterval time.Duration `yaml:"flush_interval"`

	// Timeout is the per-export-request deadline. Defaults to 30s.
	Timeout time.Duration `yaml:"timeout"`

	// Compression is the gRPC compressor to use: "gzip" or "" / "none".
	Compression string `yaml:"compression"`

	// Headers are optional gRPC metadata sent with every export (e.g. auth tokens).
	Headers map[string]string `yaml:"headers"`
}

const (
	defaultFlushInterval = time.Second
	defaultTimeout       = 30 * time.Second
)

// withDefaults returns a copy of the config with zero-valued tunables filled in.
func (c Config) withDefaults() Config {
	if c.FlushInterval <= 0 {
		c.FlushInterval = defaultFlushInterval
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
	return c
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.URL == "" {
		return errors.New("must set url")
	}
	if c.Threads < 1 {
		return errors.New("must set threads to a value greater than zero")
	}
	if c.BatchSize < 1 {
		return errors.New("must set batch_size to a value greater than zero")
	}
	switch c.Compression {
	case "", "none", "gzip":
	default:
		return errors.New("compression must be one of: none, gzip")
	}

	return nil
}
