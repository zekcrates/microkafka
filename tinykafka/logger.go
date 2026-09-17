package tinykafka

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

var levelNames = [...]string{"DEBUG", "INFO", "WARN", "ERROR"}

type Logger struct {
	Level  LogLevel
	output io.Writer
	mu     sync.Mutex
}

func NewLogger(level LogLevel) *Logger {
	return &Logger{
		Level:  level,
		output: os.Stderr,
	}
}

func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = w
}

func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if level < l.Level {
		return
	}
	msg := fmt.Sprintf(format, args...)
	t := time.Now().Format("2006-01-02 15:04:05.000")
	l.mu.Lock()
	fmt.Fprintf(l.output, "%s [%s] %s\n", t, levelNames[level], msg)
	l.mu.Unlock()
}

func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(LevelDebug, format, args...)
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.log(LevelInfo, format, args...)
}

func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(LevelWarn, format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.log(LevelError, format, args...)
}

var defaultLogger = NewLogger(LevelInfo)

func SetDefaultLogger(l *Logger) {
	defaultLogger = l
}

func LogDebug(format string, args ...interface{}) { defaultLogger.Debug(format, args...) }
func LogInfo(format string, args ...interface{})  { defaultLogger.Info(format, args...) }
func LogWarn(format string, args ...interface{})  { defaultLogger.Warn(format, args...) }
func LogError(format string, args ...interface{}) { defaultLogger.Error(format, args...) }
