package api

import (
	"time"
)

// DefaultRowsPerSecond is the smoke-test rate the presets ship with.
const DefaultRowsPerSecond = 10

// DefaultProfile is the built-in data shape, modelled on the OpenTelemetry demo
// application. It is the only profile registered by default.
const DefaultProfile = "otel_demo"

// maxBlockRows bounds a single block so memory stays predictable at high rates.
const maxBlockRows = 10_000

// OTLPLoad returns a config that generates synthetic telemetry and exports it
// over OTLP/gRPC, for pointing ClickCannon at a ClickStack collector.
//
// target is the collector's gRPC endpoint as "host:port". A leading "http://",
// "https://" or "grpc://" is accepted and stripped; "http://" also means
// plaintext. dataType must be logs or traces; OTLP export rejects profiles.
//
// The default is a smoke load of DefaultRowsPerSecond rows/s on one generator
// and one exporter. Opt into real load explicitly:
//
//	cfg := api.OTLPLoad(host+":"+port, api.DataTypeLogs)
//	cfg.WithOTLPHeaders(map[string]string{"authorization": key}).
//	    WithRowsPerSecond(100_000).
//	    WithThreads(8, 8)
func OTLPLoad(target string, dataType DataType) Config {
	cfg := Config{
		Name:     "clickcannon",
		DataType: dataType,
		LogLevel: "info",
	}

	cfg.Generate = GenerateConfig{
		Enabled:             true,
		Threads:             1,
		RowsPerSecond:       DefaultRowsPerSecond,
		RowsPerBlock:        DefaultRowsPerSecond,
		Profile:             DefaultProfile,
		ReuseBlocks:         true,
		BlockRetirementUses: 1_000,
	}

	cfg.OTel = OTelConfig{
		Enabled:       true,
		URL:           target,
		Threads:       1,
		BatchSize:     DefaultRowsPerSecond,
		FlushInterval: time.Second,
		Timeout:       30 * time.Second,
		Compression:   "gzip",
	}

	return cfg
}

// NativeInsertLoad returns a config that inserts generated telemetry straight
// into ClickHouse over the native protocol, bypassing any collector. address is
// "host:port" for the native interface: 9000 plaintext, 9440 TLS.
func NativeInsertLoad(address string, dataType DataType) Config {
	cfg := Config{
		Name:     "clickcannon",
		DataType: dataType,
		LogLevel: "info",
	}

	cfg.Generate = GenerateConfig{
		Enabled:             true,
		Threads:             1,
		RowsPerSecond:       DefaultRowsPerSecond,
		RowsPerBlock:        DefaultRowsPerSecond,
		Profile:             DefaultProfile,
		ReuseBlocks:         true,
		BlockRetirementUses: 1_000,
	}

	cfg.Insert = InsertConfig{
		Enabled:                 true,
		Threads:                 1,
		BatchSize:               1_000_000,
		WorkerRetirementBatches: 100,
		ClickHouse: ClickHouseConfig{
			Address:       address,
			Secure:        true,
			Compression:   "lz4",
			User:          "default",
			Database:      "otel",
			LogsTable:     "otel_logs",
			TracesTable:   "otel_traces",
			ProfilesTable: "otel_profiles",
		},
	}

	return cfg
}

// WithOTLPHeaders sets the gRPC metadata sent with every export, e.g. the
// collector's ingestion key.
func (c *Config) WithOTLPHeaders(headers map[string]string) *Config {
	c.OTel.Headers = headers
	return c
}

// WithRowsPerSecond caps the generator's output across all threads; 0 is
// unlimited.
//
// It also resizes the block and batch, which are not independent of the rate: a
// worker takes RowsPerBlock tokens from the limiter before emitting anything, so
// 10 rows/s with a 10,000-row block stalls ~17 minutes, and 100,000 rows/s with
// a 10-row block means 10,000 tiny RPCs per second. Use WithBlockSizes to
// override.
func (c *Config) WithRowsPerSecond(rps uint64) *Config {
	c.Generate.RowsPerSecond = rps

	size := c.blockSizeFor(rps)
	c.Generate.RowsPerBlock = size
	if c.OTel.Enabled {
		c.OTel.BatchSize = size
	}

	return c
}

// blockSizeFor targets roughly one block per generator thread per second.
func (c *Config) blockSizeFor(rowsPerSecond uint64) int {
	if rowsPerSecond == 0 {
		return maxBlockRows
	}

	threads := c.Generate.Threads
	if threads < 1 {
		threads = 1
	}

	return min(max(int(rowsPerSecond)/threads, 1), maxBlockRows)
}

// WithBlockSizes overrides the block and batch sizes; 0 leaves either unchanged.
// Call it after WithRowsPerSecond and WithThreads, which both recompute these.
func (c *Config) WithBlockSizes(rowsPerBlock, batchSize int) *Config {
	if rowsPerBlock > 0 {
		c.Generate.RowsPerBlock = rowsPerBlock
	}

	if batchSize > 0 {
		if c.OTel.Enabled {
			c.OTel.BatchSize = batchSize
		}
		if c.Insert.Enabled {
			c.Insert.BatchSize = batchSize
		}
	}

	return c
}

// WithThreads sets the generator and sink worker counts. It re-applies the block
// sizing rule, which depends on the generator thread count.
func (c *Config) WithThreads(source, sink int) *Config {
	c.Generate.Threads = source
	if c.OTel.Enabled {
		c.OTel.Threads = sink
	}
	if c.Insert.Enabled {
		c.Insert.Threads = sink
	}

	return c.WithRowsPerSecond(c.Generate.RowsPerSecond)
}
