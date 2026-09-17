package tinykafka_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"tinykafka-go/tinykafka"
)

func TestLog_AppendAndRead(t *testing.T) {
	l := setupLog(t)

	offset, err := l.AppendMessage([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Fatalf("expected offset 0, got %d", offset)
	}

	msg, err := l.ReadMessage(offset)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg, []byte("hello")) {
		t.Fatalf("expected 'hello', got %q", msg)
	}
}

func TestLog_MultipleMessages(t *testing.T) {
	l := setupLog(t)

	offsets := make([]int64, 3)
	msgs := []string{"hello", "world", "tiny kafka"}
	for i, msg := range msgs {
		offset, err := l.AppendMessage([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		offsets[i] = offset
	}

	for i, msg := range msgs {
		got, err := l.ReadMessage(offsets[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte(msg)) {
			t.Fatalf("message %d: expected %q, got %q", i, msg, got)
		}
	}
}

func TestLog_AppendReturnsOffset(t *testing.T) {
	l := setupLog(t)

	o1, _ := l.AppendMessage([]byte("abc"))
	o2, _ := l.AppendMessage([]byte("de"))
	o3, _ := l.AppendMessage([]byte("f"))

	if o1 != 0 {
		t.Fatalf("expected first offset 0, got %d", o1)
	}
	if o2 != 7 {
		t.Fatalf("expected second offset 7 (4+3), got %d", o2)
	}
	if o3 != 13 {
		t.Fatalf("expected third offset 13 (4+3+4+2), got %d", o3)
	}
}

func TestLog_ReopenExistingLog(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "reopen.log")

	l1 := &tinykafka.Log{Filename: path}
	if err := l1.Open(); err != nil {
		t.Fatal(err)
	}
	l1.AppendMessage([]byte("first"))
	l1.AppendMessage([]byte("second"))
	l1.Close()

	l2 := &tinykafka.Log{Filename: path}
	if err := l2.Open(); err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	if l2.NextOffset == 0 {
		t.Fatal("expected NextOffset to be non-zero after reopen")
	}

	msg, err := l2.ReadMessage(0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg, []byte("first")) {
		t.Fatalf("expected 'first', got %q", msg)
	}
}

func TestLog_ReadFromInvalidOffset(t *testing.T) {
	l := setupLog(t)

	_, err := l.ReadMessage(9999)
	if err == nil {
		t.Fatal("expected error reading from invalid offset")
	}
}

func TestLog_AppendLargeMessage(t *testing.T) {
	l := setupLog(t)

	large := make([]byte, 65536)
	for i := range large {
		large[i] = byte(i % 256)
	}

	offset, err := l.AppendMessage(large)
	if err != nil {
		t.Fatal(err)
	}

	msg, err := l.ReadMessage(offset)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg, large) {
		t.Fatal("large message mismatch")
	}
}

func TestLog_AppendEmptyMessage(t *testing.T) {
	l := setupLog(t)

	offset, err := l.AppendMessage([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	msg, err := l.ReadMessage(offset)
	if err != nil {
		t.Fatal(err)
	}
	if len(msg) != 0 {
		t.Fatalf("expected empty message, got %d bytes", len(msg))
	}
}

func TestLog_CloseIdempotent(t *testing.T) {
	l := setupLog(t)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal("second close should not error")
	}
}
