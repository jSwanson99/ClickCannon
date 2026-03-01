package app

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"otelspam/internal/disk"
	"otelspam/internal/insert"
	"otelspam/internal/metrics"
	"otelspam/internal/user"
	"path/filepath"

	"github.com/goccy/go-yaml"
)

const ConfigDataTypeLogs = "logs"
const ConfigDataTypeTraces = "traces"

type Config struct {
	App struct {
		Name         string `yaml:"name"`
		LogToFile    bool   `yaml:"log_to_file"`
		LogToConsole bool   `yaml:"log_to_console"`
		LogLevel     string `yaml:"log_level"`
		DataType     string `yaml:"data_type"`
		// TODO: disk might need these for time shift
		//DatasetUnixStart uint64 `yaml:"dataset_unix_start"`
		//DatasetUnixEnd   uint64 `yaml:"dataset_unix_end"`
		//DatasetRowCount  uint64 `yaml:"dataset_row_count"`
		Seed string `yaml:"seed"`
	} `yaml:"app"`

	Disk    disk.Config    `yaml:"disk"`
	Insert  insert.Config  `yaml:"insert"`
	Metrics metrics.Config `yaml:"metrics"`
	User    user.Config    `yaml:"user"`
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
		return fmt.Errorf("insert: %w", err)
	}

	if err := c.Metrics.Validate(); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}

	if err := c.User.Validate(); err != nil {
		return fmt.Errorf("user: %w", err)
	}

	return nil
}

func LoadConfig() (*Config, string, error) {
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to YAML configuration file")
	flag.Parse()

	if configPath == "" {
		return nil, "", errors.New("--config flag is required")
	}

	configFileName := filepath.Base(configPath)

	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		return nil, "", err
	}

	var config Config
	err = yaml.Unmarshal(configBytes, &config)
	if err != nil {
		return nil, "", err
	}

	err = config.Validate()
	if err != nil {
		return nil, "", fmt.Errorf("invalid config: %w", err)
	}

	return &config, configFileName, nil
}
