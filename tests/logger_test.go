package tinykafka_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"tinykafka-go/tinykafka"
)

func captureOutput(fn func()) string {
	r, w, _ := os.Pipe()
	orig := os.Stderr
	os.Stderr = w
	fn()
	w.Close()
	os.Stderr = orig
	var buf bytes.Buffer
	buf.ReadFrom(r)
	return buf.String()
}

func newLoggerWithCapture(level tinykafka.LogLevel) (*tinykafka.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	l := tinykafka.NewLogger(level)
	l.SetOutput(&buf)
	return l, &buf
}

func TestNewLogger(t *testing.T) {
	l := tinykafka.NewLogger(tinykafka.LevelWarn)
	if l == nil {
		t.Fatal("NewLogger returned nil")
	}
}

func TestLogger_InfoAtInfoLevel(t *testing.T) {
	l, buf := newLoggerWithCapture(tinykafka.LevelInfo)
	l.Info("hello %s", "world")
	output := buf.String()
	if !strings.Contains(output, "[INFO]") {
		t.Fatalf("expected [INFO] in output, got: %s", output)
	}
	if !strings.Contains(output, "hello world") {
		t.Fatalf("expected 'hello world' in output, got: %s", output)
	}
}

func TestLogger_DebugFilteredAtInfoLevel(t *testing.T) {
	l, buf := newLoggerWithCapture(tinykafka.LevelInfo)
	l.Debug("should not appear")
	output := buf.String()
	if strings.Contains(output, "should not appear") {
		t.Fatalf("debug message should be filtered at info level, got: %s", output)
	}
}

func TestLogger_DebugAtDebugLevel(t *testing.T) {
	l, buf := newLoggerWithCapture(tinykafka.LevelDebug)
	l.Debug("debug msg")
	output := buf.String()
	if !strings.Contains(output, "[DEBUG]") {
		t.Fatalf("expected [DEBUG] in output, got: %s", output)
	}
	if !strings.Contains(output, "debug msg") {
		t.Fatalf("expected 'debug msg' in output, got: %s", output)
	}
}

func TestLogger_Warn(t *testing.T) {
	l, buf := newLoggerWithCapture(tinykafka.LevelWarn)
	l.Warn("warning %d", 42)
	output := buf.String()
	if !strings.Contains(output, "[WARN]") {
		t.Fatalf("expected [WARN] in output, got: %s", output)
	}
	if !strings.Contains(output, "warning 42") {
		t.Fatalf("expected 'warning 42' in output, got: %s", output)
	}
}

func TestLogger_Error(t *testing.T) {
	l, buf := newLoggerWithCapture(tinykafka.LevelError)
	l.Error("error occurred")
	output := buf.String()
	if !strings.Contains(output, "[ERROR]") {
		t.Fatalf("expected [ERROR] in output, got: %s", output)
	}
	if !strings.Contains(output, "error occurred") {
		t.Fatalf("expected 'error occurred' in output, got: %s", output)
	}
}

func TestLogger_ErrorVisibleAtWarnLevel(t *testing.T) {
	l, buf := newLoggerWithCapture(tinykafka.LevelWarn)
	l.Error("should appear")
	output := buf.String()
	if !strings.Contains(output, "should appear") {
		t.Fatalf("error message should appear at warn level, got: %s", output)
	}
}

func TestLogger_InfoFilteredAtErrorLevel(t *testing.T) {
	l, buf := newLoggerWithCapture(tinykafka.LevelError)
	l.Info("should not appear")
	output := buf.String()
	if strings.Contains(output, "should not appear") {
		t.Fatalf("info message should be filtered at error level, got: %s", output)
	}
}

func TestLogger_TimestampFormat(t *testing.T) {
	l, buf := newLoggerWithCapture(tinykafka.LevelDebug)
	l.Info("test")
	output := buf.String()
	if !strings.Contains(output, "[INFO] test") {
		t.Fatalf("unexpected output format: %s", output)
	}
}

func TestSetDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
	l := tinykafka.NewLogger(tinykafka.LevelDebug)
	l.SetOutput(&buf)
	tinykafka.SetDefaultLogger(l)
	tinykafka.LogInfo("package level log")
	output := buf.String()
	if !strings.Contains(output, "package level log") {
		t.Fatalf("expected 'package level log' in output, got: %s", output)
	}
}

func TestPackageLevelFunctions(t *testing.T) {
	var buf bytes.Buffer
	l := tinykafka.NewLogger(tinykafka.LevelDebug)
	l.SetOutput(&buf)
	tinykafka.SetDefaultLogger(l)
	tinykafka.LogDebug("debug pkg")
	tinykafka.LogInfo("info pkg")
	tinykafka.LogWarn("warn pkg")
	tinykafka.LogError("error pkg")
	output := buf.String()
	if !strings.Contains(output, "debug pkg") {
		t.Fatalf("missing debug pkg in output: %s", output)
	}
	if !strings.Contains(output, "info pkg") {
		t.Fatalf("missing info pkg in output: %s", output)
	}
	if !strings.Contains(output, "warn pkg") {
		t.Fatalf("missing warn pkg in output: %s", output)
	}
	if !strings.Contains(output, "error pkg") {
		t.Fatalf("missing error pkg in output: %s", output)
	}
}
