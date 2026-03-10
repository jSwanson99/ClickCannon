package disk

import "errors"

type Config struct {
	Enabled bool `yaml:"enabled"`
	// How many read threads
	Threads int `yaml:"threads"`

	LogsPath   string `yaml:"logs_path"`
	TracesPath string `yaml:"traces_path"`

	ReuseBlocks bool `yaml:"reuse_blocks"`

	Loop bool `yaml:"loop"`

	// How many MiB decompressed bytes per second can be read from disk
	MiBytesPerSecondLimit uint64 `yaml:"mb_per_second_limit"`

	ShiftTimestamp string `yaml:"shift_timestamp"`

	// Whether the disk files contain the TimestampTime column.
	// TimestampTime is derived from Timestamp on insert, so it is never inserted.
	// Set to true if the files on disk were exported with TimestampTime present.
	HasTimestampTime bool `yaml:"has_timestamp_time"`
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.Threads < 1 {
		return errors.New("must set threads to a value greater than zero")
	}

	if c.LogsPath == "" {
		return errors.New("logs_path is empty")
	}

	if c.TracesPath == "" {
		return errors.New("traces_path is empty")
	}

	if c.MiBytesPerSecondLimit < 1 {
		return errors.New("must set mb_per_second_limit to a value greater than zero")
	}

	if c.ShiftTimestamp == "" {
		c.ShiftTimestamp = ShiftTimestampNone
	} else if c.ShiftTimestamp != ShiftTimestampNone && c.ShiftTimestamp != ShiftTimestampDate && c.ShiftTimestamp != ShiftTimestampAll && c.ShiftTimestamp != ShiftTimestampNow && c.ShiftTimestamp != ShiftTimestampMinute {
		return errors.New("must set shift_timestamp to one of: <empty>, none, date, all, now")
	}

	return nil
}
