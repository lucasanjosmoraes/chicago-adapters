package logrusadapter

import (
	"context"
	"os"

	"github.com/lucasanjosmoraes/chicago-ports/pkg/log"
	"github.com/sirupsen/logrus"
)

// Config stores all logger configuration variables.
type Config struct {
	LogLevel string
	Version  string
}

// Adapter is a Logger implementation based on logrus dependency.
type Adapter struct {
	Config Config
	logger *logrus.Entry
}

// New instantiates a new Logger, but also helps us to validate if Adapter implements
// log.Logger correctly.
func New(c Config) log.Logger {
	logLevel, err := logrus.ParseLevel(c.LogLevel)
	if err != nil {
		logLevel = logrus.DebugLevel
	}

	logrus.SetFormatter(&logrus.JSONFormatter{
		DisableHTMLEscape: true,
	})
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logLevel)

	return Adapter{
		Config: c,
		logger: logrus.WithField("version", c.Version),
	}
}

// Debug will log the given arguments at debug level.
func (a Adapter) Debug(ctx context.Context, args ...interface{}) {
	a.withCorrelationID(ctx).Debug(args...)
}

// Debugf will log the given arguments on the given format at debug level.
func (a Adapter) Debugf(ctx context.Context, format string, args ...interface{}) {
	a.withCorrelationID(ctx).Debugf(format, args...)
}

// Error will log the given arguments at error level.
func (a Adapter) Error(ctx context.Context, args ...interface{}) {
	a.withCorrelationID(ctx).Error(args...)
}

// Errorf will log the given arguments on the given format at error level.
func (a Adapter) Errorf(ctx context.Context, format string, args ...interface{}) {
	a.withCorrelationID(ctx).Errorf(format, args...)
}

// Fatal will log the given arguments and call os.Exit.
func (a Adapter) Fatal(ctx context.Context, args ...interface{}) {
	a.withCorrelationID(ctx).Fatal(args...)
}

// Fatalf will log the given arguments on the given format and call os.Exit.
func (a Adapter) Fatalf(ctx context.Context, format string, args ...interface{}) {
	a.withCorrelationID(ctx).Fatalf(format, args...)
}

// Info will log the given arguments at info level.
func (a Adapter) Info(ctx context.Context, args ...interface{}) {
	a.withCorrelationID(ctx).Info(args...)
}

// Infof will log the given arguments on the given format at info level.
func (a Adapter) Infof(ctx context.Context, format string, args ...interface{}) {
	a.withCorrelationID(ctx).Infof(format, args...)
}

// Panic will log the given arguments and panic.
func (a Adapter) Panic(ctx context.Context, args ...interface{}) {
	a.withCorrelationID(ctx).Panic(args...)
}

// Panicf will log the given arguments on the given format and panic.
func (a Adapter) Panicf(ctx context.Context, format string, args ...interface{}) {
	a.withCorrelationID(ctx).Panicf(format, args...)
}

// Warn will log the given arguments at warn level.
func (a Adapter) Warn(ctx context.Context, args ...interface{}) {
	a.withCorrelationID(ctx).Warn(args...)
}

// Warnf will log the given arguments on the given format at warn level.
func (a Adapter) Warnf(ctx context.Context, format string, args ...interface{}) {
	a.withCorrelationID(ctx).Warnf(format, args...)
}

func (a Adapter) withCorrelationID(ctx context.Context) *logrus.Entry {
	if ctx == nil {
		ctx = context.Background()
	}

	return a.logger.WithField("correlationID", ctx.Value("correlationID"))
}
