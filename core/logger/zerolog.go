package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cshekharsharma/photon/core/logger/writers"
	"github.com/cshekharsharma/photon/utils/types"
	zerologLib "github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
)

var configureZerologGlobalsOnce sync.Once

var (
	fileLoggerStat     = os.Stat
	fileLoggerMkdir    = os.Mkdir
	fileLoggerOpenFile = os.OpenFile
)

// zerolog is a wrapper around zerologLib.Logger.
type zerolog struct {
	logger *zerologLib.Logger
}

// newZerolog creates a new instance of zerolog based on the provided configuration.
// It configures the logger to write to stdout and/or a file based on the config.
// Returns an error if no valid logger could be configured.
func newZerolog(config *LoggerConfig) (*zerolog, error) {
	var leaves []io.Writer

	configureZerologGlobalsOnce.Do(func() {
		zerologLib.ErrorStackMarshaler = pkgerrors.MarshalStack
		zerologLib.TimeFieldFormat = time.DateTime
	})

	if config.Type&LoggerTypeStdout != 0 {
		leaves = append(leaves, configureConsoleLogger(config.Writer, config.TimeFormat))
	}

	if config.Type&LoggerTypeFile != 0 {
		filewriter, err := configureFileLogger(config)
		if err != nil {
			return &zerolog{}, err
		}
		leaves = append(leaves, filewriter)
	}

	if config.Type&LoggerTypeOtel != 0 {
		otelWriter := configureOtelLogger(config)
		leaves = append(leaves, otelWriter)
	}

	if len(leaves) == 0 {
		return &zerolog{}, fmt.Errorf("no logger could be configured for `%s`", config.Name)
	}

	multi := zerologLib.MultiLevelWriter(leaves...)
	logger := zerologLib.New(multi).With().Timestamp().Logger()

	configuredLogger := logger.Level(parseLogLevel(string(config.Level)))
	return &zerolog{
		logger: &configuredLogger,
	}, nil
}

// configureConsoleLogger configures a zerolog console writer with standard settings.
func configureConsoleLogger(ww io.Writer, timeFormats ...string) io.Writer {
	writer := zerologLib.NewConsoleWriter()
	writer.TimeFormat = time.DateTime
	writer.Out = os.Stderr
	writer.NoColor = false

	if len(timeFormats) > 0 && timeFormats[0] != "" {
		writer.TimeFormat = timeFormats[0]
	}
	if ww != nil {
		writer.Out = ww
	}

	return writer
}

// configureFileLogger configures a zerolog file writer based on the provided configuration.
// Creates the base directory if it does not exist.
func configureFileLogger(config *LoggerConfig) (io.Writer, error) {
	if len(config.BaseDir) < 1 {
		config.BaseDir = os.TempDir()
	}

	if _, err := fileLoggerStat(config.BaseDir); os.IsNotExist(err) {
		err := fileLoggerMkdir(config.BaseDir, 0750)
		if err != nil {
			return nil, err
		}
	}

	filename := fmt.Sprintf("%s.log", strings.ToLower(config.Name))
	fullpath := filepath.Join(config.BaseDir, filename)

	file, err := fileLoggerOpenFile(fullpath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600) // #nosec G304 -- path is built from caller configured log directory and logger name.
	if err != nil {
		return nil, err
	}

	return file, nil
}

// configureOtelLogger creates a writer that pushes logs to OpenTelemetry via OTLP.
func configureOtelLogger(config *LoggerConfig) io.Writer {
	writer := writers.NewOtelWriter(config.Name)

	if config.Writer != nil {
		if _, ok := config.Writer.(*writers.OtelWriter); ok {
			return config.Writer
		}
	}
	return writer
}

// parseLogLevel converts a string log level to a zerolog.Level.
// Returns the default log level if the provided level is invalid.
func parseLogLevel(level string) zerologLib.Level {
	defaultLogLevel, _ := zerologLib.ParseLevel(DefaultLogLevel)

	if isValidLogLevel(level) {
		defaultLogLevel, _ = zerologLib.ParseLevel(level)
	}

	return defaultLogLevel
}

// isValidLogLevel checks if the provided log level is valid.
func isValidLogLevel(level string) bool {
	validLevels := []string{
		zerologLib.LevelTraceValue,
		zerologLib.LevelDebugValue,
		zerologLib.LevelInfoValue,
		zerologLib.LevelWarnValue,
		zerologLib.LevelErrorValue,
		zerologLib.LevelFatalValue,
		zerologLib.LevelPanicValue,
	}

	isValid, _ := types.ExistsInList(level, validLevels)
	return isValid
}

// With returns a new logger with additional context fields.
func (z *zerolog) With(fields map[string]interface{}) Logger {
	childLogger := z.logger.With().Fields(fields).Logger()
	return &zerolog{
		logger: &childLogger,
	}
}

// TraceWithFields logs a trace message with additional context fields.
func (z *zerolog) TraceWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	z.logger.Trace().Fields(fields).Msgf(message, args...)
}

// Trace logs a trace message.
func (u *zerolog) Trace(message string, args ...interface{}) {
	u.logger.Trace().Msgf(message, args...)
}

// DebugWithFields logs a debug message with additional context fields.
func (z *zerolog) DebugWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	z.logger.Debug().Fields(fields).Msgf(message, args...)
}

// Debug logs a debug message.
func (u *zerolog) Debug(message string, args ...interface{}) {
	u.logger.Debug().Msgf(message, args...)
}

// InfoWithFields logs an info message with additional context fields.
func (z *zerolog) InfoWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	z.logger.Info().Fields(fields).Msgf(message, args...)
}

// Info logs an info message.
func (u *zerolog) Info(message string, args ...interface{}) {
	u.logger.Info().Msgf(message, args...)
}

// WarnWithFields logs a warning message with additional context fields.
func (z *zerolog) WarnWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	z.logger.Warn().Fields(fields).Msgf(message, args...)
}

// Warn logs a warning message.
func (u *zerolog) Warn(message string, args ...interface{}) {
	u.logger.Warn().Msgf(message, args...)
}

// ErrorWithFields logs an error message with additional context fields.
func (z *zerolog) ErrorWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	z.logger.Error().Fields(fields).Msgf(message, args...)
}

// Error logs an error message.
func (u *zerolog) Error(message string, args ...interface{}) {
	u.logger.Error().Msgf(message, args...)
}

// FatalWithFields logs a fatal message with additional context fields and exits the application.
func (z *zerolog) FatalWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	z.logger.Fatal().Fields(fields).Msgf(message, args...)
}

// Fatal logs a fatal message and exits the application.
func (u *zerolog) Fatal(message string, args ...interface{}) {
	u.logger.Fatal().Msgf(message, args...)
}

// PanicWithFields logs a panic message with additional context fields and panics.
func (z *zerolog) PanicWithFields(fields map[string]interface{}, message string, args ...interface{}) {
	z.logger.Panic().Fields(fields).Msgf(message, args...)
}

// Panic logs a panic message and panics.
func (u *zerolog) Panic(message string, args ...interface{}) {
	u.logger.Panic().Msgf(message, args...)
}

// LogWithFields logs a message at the specified level with additional context fields.
func (u *zerolog) LogWithFields(level LogLevel, fields map[string]interface{}, message string, args ...interface{}) {
	zlevel, _ := zerologLib.ParseLevel(string(level))
	u.logger.WithLevel(zlevel).Fields(fields).Msgf(message, args...)
}

// Log logs a message at the specified level.
func (u *zerolog) Log(level LogLevel, message string, args ...interface{}) {
	zlevel, _ := zerologLib.ParseLevel(string(level))
	u.logger.WithLevel(zlevel).Msgf(message, args...)
}
