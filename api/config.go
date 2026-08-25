// Package api is the public wrapper around ClickCannon, for embedding it as a
// library. Unlike the binary it never calls flag.Parse, os.Exit, signal.Notify,
// or mutates time.Local, and it returns errors instead of printing them.
package api

import (
	"fmt"

	"github.com/ClickHouse/ClickCannon/internal/app"
	"github.com/ClickHouse/ClickCannon/internal/disk"
	"github.com/ClickHouse/ClickCannon/internal/generate"
	"github.com/ClickHouse/ClickCannon/internal/insert"
	"github.com/ClickHouse/ClickCannon/internal/metrics"
	"github.com/ClickHouse/ClickCannon/internal/otel"
	"github.com/ClickHouse/ClickCannon/internal/user"

	"github.com/goccy/go-yaml"
)

// DataType selects which signal a run produces. A run handles exactly one.
type DataType = string

const (
	DataTypeLogs     DataType = app.ConfigDataTypeLogs
	DataTypeTraces   DataType = app.ConfigDataTypeTraces
	DataTypeProfiles DataType = app.ConfigDataTypeProfiles
)

// Config is the whole configuration, identical to the binary's. Exactly one of
// Disk/Generate may be enabled. If both sinks are enabled, OTel wins and Insert
// is disabled: only one sink can drain the block queue.
//
// App.LogToFile, App.LogToConsole and App.LogLevel are only read by the binary,
// which builds the logger passed to New. A library caller configures its own.
type Config = app.Config

// Aliases of the internal config types, so callers can name them in composite
// literals. Fields are documented on the internal types.
type (
	AppConfig      = app.AppConfig
	DiskConfig     = disk.Config
	GenerateConfig = generate.Config
	TracesConfig   = generate.TracesConfig
	ProfilesConfig = generate.ProfilesConfig

	InsertConfig     = insert.Config
	ClickHouseConfig = insert.ClickHouseConfig

	OTelConfig = otel.Config

	MetricsConfig = metrics.Config

	UserConfig      = user.Config
	UserWorkflow    = user.WorkflowBaseConfig
	QueriesWorkflow = user.QueriesWorkflowConfig
	QueryConfig     = user.QueryConfig
	TimeRangeConfig = user.TimeRangeConfig
)

// ParseYAML reads and validates a config in the format the binary's --config
// flag accepts.
func ParseYAML(data []byte) (Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse clickcannon config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid clickcannon config: %w", err)
	}

	return cfg, nil
}

// ToYAML renders a config in the binary's file format.
func ToYAML(cfg Config) ([]byte, error) {
	return yaml.Marshal(cfg)
}
