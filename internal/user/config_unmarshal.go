package user

import (
	"context"
	"fmt"
)

func (b *BehaviorBaseConfig) UnmarshalYAML(ctx context.Context, unmarshal func(any) error) error {
	var base struct {
		Type string `yaml:"type"`
		Name string `yaml:"name"`
	}
	if err := unmarshal(&base); err != nil {
		return err
	}
	b.Type = base.Type
	b.Name = base.Name

	var (
		cfg BehaviorConfig
		err error
	)
	switch base.Type {
	case "queries":
		cfg, err = unmarshalBehavior[QueriesBehaviorConfig](unmarshal)
	case "har":
		cfg, err = unmarshalBehavior[HARBehaviorConfig](unmarshal)
	default:
		return fmt.Errorf("unknown behavior type %q", base.Type)
	}

	if err != nil {
		return err
	}

	b.Config = cfg
	return nil
}

func unmarshalBehavior[T BehaviorConfig](unmarshal func(any) error) (T, error) {
	var cfg T
	if err := unmarshal(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *TimeRangeConfig) UnmarshalYAML(ctx context.Context, unmarshal func(any) error) error {
	type plain TimeRangeConfig
	var p plain
	if err := unmarshal(&p); err != nil {
		return err
	}

	if p.Type == "" {
		p.Type = TimeRangeUniform
	}

	*c = TimeRangeConfig(p)
	return nil
}
