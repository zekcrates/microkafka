package tinykafka_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"tinykafka-go/tinykafka"
)

func TestPartition_AppendAndRead(t *testing.T) {
	l := &tinykafka.Log{Filename: filepath.Join(tempDir(t), "test.log")}
	if err := l.Open(); err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	p := &tinykafka.Partition{Log: l}

	offset, err := p.AppendMessage([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	msg, err := p.ReadMessage(offset)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg, []byte("hello")) {
		t.Fatalf("expected 'hello', got %q", msg)
	}
}

func TestPartition_DelegatesToLog(t *testing.T) {
	l := &tinykafka.Log{Filename: filepath.Join(tempDir(t), "test.log")}
	if err := l.Open(); err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	p := &tinykafka.Partition{Log: l}

	o1, _ := p.AppendMessage([]byte("aaa"))
	o2, _ := p.AppendMessage([]byte("bb"))

	if o1 != 0 {
		t.Fatalf("expected offset 0, got %d", o1)
	}
	if o2 != 7 {
		t.Fatalf("expected offset 7, got %d", o2)
	}

	msg1, _ := p.ReadMessage(o1)
	msg2, _ := p.ReadMessage(o2)

	if !bytes.Equal(msg1, []byte("aaa")) {
		t.Fatalf("expected 'aaa', got %q", msg1)
	}
	if !bytes.Equal(msg2, []byte("bb")) {
		t.Fatalf("expected 'bb', got %q", msg2)
	}
}
