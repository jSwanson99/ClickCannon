// Package api is the public wrapper around ClickCannon, for embedding it as a
// library. Unlike the binary it never calls flag.Parse, os.Exit, signal.Notify,
// or mutates time.Local, and it returns errors instead of printing them.
package api

import (
	"fmt"
	"os"

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

// Aliases of the internal config types, so YAML tags, defaults and validation
// are identical to the binary's
type (
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

// Config mirrors the YAML config file, with the file's `app:` block flattened
// into the top-level fields. The internal representation uses an anonymous
// struct there, which callers outside this module cannot write a literal for.
//
// Exactly one of Disk/Generate may be enabled. If both sinks are enabled, OTel
// wins and Insert is disabled: only one sink can drain the block queue.
type Config struct {
	Name     string
	DataType DataType
	Seed     string
	// LogLevel is only consulted by the binary, which builds the logger passed
	// to New. A library caller sets the level on its own logger.
	LogLevel string

	Disk     DiskConfig
	Generate GenerateConfig

	Insert InsertConfig
	OTel   OTelConfig

	Metrics MetricsConfig
	User    UserConfig
}

func (c *Config) Validate() error {
	internal := c.toInternal()
	if err := internal.Validate(); err != nil {
		return err
	}

	// generate.Config.Validate defaults fields in place; copy them back so the
	// caller sees what will actually run.
	c.Generate = internal.Generate

	return nil
}

func (c *Config) toInternal() *app.Config {
	out := &app.Config{
		Disk:     c.Disk,
		Generate: c.Generate,
		Insert:   c.Insert,
		OTel:     c.OTel,
		Metrics:  c.Metrics,
		User:     c.User,
	}
	out.App.Name = c.Name
	out.App.DataType = c.DataType
	out.App.Seed = c.Seed
	out.App.LogLevel = c.LogLevel

	// Logging is the host program's business.
	out.App.LogToFile = false
	out.App.LogToConsole = false

	return out
}

func fromInternal(in *app.Config) Config {
	return Config{
		Name:     in.App.Name,
		DataType: in.App.DataType,
		Seed:     in.App.Seed,
		LogLevel: in.App.LogLevel,
		Disk:     in.Disk,
		Generate: in.Generate,
		Insert:   in.Insert,
		OTel:     in.OTel,
		Metrics:  in.Metrics,
		User:     in.User,
	}
}

// FromCLIConfig adapts the binary's parsed config so it drives the same
// pipeline. External callers cannot name the argument type and should use
// ParseYAML or LoadYAMLFile.
func FromCLIConfig(cfg *app.Config) Config {
	return fromInternal(cfg)
}

// ParseYAML reads and validates a config in the format the binary's --config
// flag accepts.
func ParseYAML(data []byte) (Config, error) {
	var internal app.Config
	if err := yaml.Unmarshal(data, &internal); err != nil {
		return Config{}, fmt.Errorf("parse clickcannon config: %w", err)
	}

	cfg := fromInternal(&internal)
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid clickcannon config: %w", err)
	}

	return cfg, nil
}

func LoadYAMLFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read clickcannon config %q: %w", path, err)
	}

	return ParseYAML(data)
}

// ToYAML renders the config in the binary's file format.
func (c *Config) ToYAML() ([]byte, error) {
	return yaml.Marshal(c.toInternal())
}

func NewRunID() string {
	return app.NewRunID()
}
