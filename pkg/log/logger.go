package log

import (
	"log/slog"
	"os"
)

func SetupLogger(appName string, debug bool, file *string) (*slog.Logger, error) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	writer := os.Stdout
	if file != nil && *file != "" {
		f, err := os.OpenFile(*file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		writer = f
	}

	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{
					Key:   "timestamp",
					Value: slog.StringValue(attr.Value.Time().Format("15:04.00")),
				}
			}
			return attr
		},
	})

	logger := slog.New(handler).With(
		slog.String("app", appName),
	)

	return logger, nil
}
