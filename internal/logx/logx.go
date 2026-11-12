package logx

import (
	"log/slog"
	"os"

	"sentinel/internal/config"
)

func New(service string, c config.Log) *slog.Logger {
	opts := &slog.HandlerOptions{Level: c.Level}
	var h slog.Handler
	if c.Format == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(h).With("service", service)
}
