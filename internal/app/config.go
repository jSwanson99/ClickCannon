package app

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"otelspam/internal/disk"
	"otelspam/internal/insert"
	"otelspam/internal/metrics"

	"github.com/goccy/go-yaml"
)

const ConfigDataTypeLogs = "logs"
const ConfigDataTypeTraces = "traces"

type Config struct {
	App struct {
		LogToFile    bool   `yaml:"log_to_file"`
		LogToConsole bool   `yaml:"log_to_console"`
		LogLevel     string `yaml:"log_level"`
		DataType     string `yaml:"data_type"`
		Seed         string `yaml:"seed"`
	} `yaml:"app"`

	Disk    disk.Config    `yaml:"disk"`
	Insert  insert.Config  `yaml:"insert"`
	Metrics metrics.Config `yaml:"metrics"`
}

func (c Config) GetDataFolder() string {
	switch c.App.DataType {
	case ConfigDataTypeLogs:
		return c.Disk.LogsPath
	case ConfigDataTypeTraces:
		return c.Disk.TracesPath
	default:
		return ""
	}
}

func (c Config) GetInsertTable() string {
	switch c.App.DataType {
	case ConfigDataTypeLogs:
		return c.Insert.ClickHouse.LogsTable
	case ConfigDataTypeTraces:
		return c.Insert.ClickHouse.TracesTable
	default:
		return ""
	}
}

func (c Config) IsLogsData() bool {
	return c.App.DataType == ConfigDataTypeLogs
}

func (c Config) Validate() error {
	if c.App.DataType == "" || (c.App.DataType != ConfigDataTypeLogs && c.App.DataType != ConfigDataTypeTraces) {
		return errors.New("app: data_type must be one of: logs, traces")
	}

	if err := c.Disk.Validate(); err != nil {
		return fmt.Errorf("disk: %w", err)
	}

	if err := c.Insert.Validate(); err != nil {
		return fmt.Errorf("disk: %w", err)
	}

	if err := c.Metrics.Validate(); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}

	return nil
}

func LoadConfig() (*Config, error) {
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to YAML configuration file")
	flag.Parse()

	if configPath == "" {
		return nil, errors.New("--config flag is required")
	}

	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(configBytes, &config)
	if err != nil {
		return nil, err
	}

	err = config.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &config, nil
}
