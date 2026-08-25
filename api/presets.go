package api

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
//	cfg.OTel.Headers = map[string]string{"authorization": key}
//	api.SetThreads(&cfg, 8, 8)
//	api.SetRowsPerSecond(&cfg, 100_000)
func OTLPLoad(target string, dataType DataType) Config {
	cfg := Config{
		App: AppConfig{
			Name:     "clickcannon",
			DataType: dataType,
		},
	}

	cfg.Generate = GenerateConfig{
		Enabled:             true,
		Threads:             1,
		RowsPerSecond:       DefaultRowsPerSecond,
		RowsPerBlock:        DefaultRowsPerSecond,
		Profile:             DefaultProfile,
		ReuseBlocks:         true,
		BlockRetirementUses: 50,
	}

	// FlushInterval and Timeout are left zero: otel.Config.withDefaults fills
	// them with 1s and 30s.
	cfg.OTel = OTelConfig{
		Enabled:     true,
		URL:         target,
		Threads:     1,
		BatchSize:   DefaultRowsPerSecond,
		Compression: "gzip",
	}

	return cfg
}

// SetRowsPerSecond caps the generator's output across all threads; 0 is
// unlimited.
//
// It also resizes the block and batch, which are not independent of the rate: a
// worker takes RowsPerBlock tokens from the limiter before emitting anything, so
// 10 rows/s with a 10,000-row block stalls ~17 minutes, and 100,000 rows/s with
// a 10-row block means 10,000 tiny RPCs per second. Set the sizes afterwards to
// override.
func SetRowsPerSecond(cfg *Config, rps uint64) {
	cfg.Generate.RowsPerSecond = rps

	size := blockSizeFor(cfg, rps)
	cfg.Generate.RowsPerBlock = size
	if cfg.OTel.Enabled {
		cfg.OTel.BatchSize = size
	}
}

// SetThreads sets the generator and sink worker counts. It re-applies the block
// sizing rule, which depends on the generator thread count.
func SetThreads(cfg *Config, source, sink int) {
	cfg.Generate.Threads = source
	if cfg.OTel.Enabled {
		cfg.OTel.Threads = sink
	}
	if cfg.Insert.Enabled {
		cfg.Insert.Threads = sink
	}

	SetRowsPerSecond(cfg, cfg.Generate.RowsPerSecond)
}

// blockSizeFor targets roughly one block per generator thread per second.
func blockSizeFor(cfg *Config, rowsPerSecond uint64) int {
	if rowsPerSecond == 0 {
		return maxBlockRows
	}

	threads := cfg.Generate.Threads
	if threads < 1 {
		threads = 1
	}

	return min(max(int(rowsPerSecond)/threads, 1), maxBlockRows)
}
