package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

func ParseLogLevel(s string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo, fmt.Errorf("invalid log level %q: %w", s, err)
	}

	return level, nil
}

func NewLogger(logPath string, level slog.Level, logToConsole, logToFile bool) (*slog.Logger, *os.File, error) {
	opts := &slog.HandlerOptions{Level: level}

	var handlers []slog.Handler
	if logToConsole {
		handlers = append(handlers, slog.NewTextHandler(os.Stdout, opts))
	}

	var (
		f   *os.File
		err error
	)
	if logToFile {
		f, err = os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, nil, fmt.Errorf("opening log file: %w", err)
		}

		handlers = append(handlers, slog.NewTextHandler(f, opts))
	}

	multiHandler := NewMultiLogHandler(handlers...)

	return slog.New(multiHandler), f, nil
}

type MultiLogHandler struct {
	handlers []slog.Handler
}

func NewMultiLogHandler(handlers ...slog.Handler) *MultiLogHandler {
	return &MultiLogHandler{handlers: handlers}
}

func (h *MultiLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *MultiLogHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, handler := range h.handlers {
		if err := handler.Handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (h *MultiLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, h := range h.handlers {
		handlers[i] = h.WithAttrs(attrs)
	}
	return NewMultiLogHandler(handlers...)
}

func (h *MultiLogHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, h := range h.handlers {
		handlers[i] = h.WithGroup(name)
	}
	return NewMultiLogHandler(handlers...)
}
