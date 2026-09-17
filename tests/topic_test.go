package tinykafka_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"tinykafka-go/tinykafka"
)

func TestTopic_AppendToPartition(t *testing.T) {
	dir := tempDir(t)
	topic := &tinykafka.Topic{Name: "orders"}
	for i := 0; i < 2; i++ {
		topic.Partitions = append(topic.Partitions, &tinykafka.Partition{
			Log: &tinykafka.Log{Filename: filepath.Join(dir, "orders-"+string(rune('0'+i))+".log")},
		})
	}
	if err := topic.Open(); err != nil {
		t.Fatal(err)
	}
	defer topic.Close()

	offset0, err := topic.AppendMessage(0, []byte("msg-p0"))
	if err != nil {
		t.Fatal(err)
	}
	offset1, err := topic.AppendMessage(1, []byte("msg-p1"))
	if err != nil {
		t.Fatal(err)
	}

	if offset0 != 0 {
		t.Fatalf("expected partition 0 offset 0, got %d", offset0)
	}
	if offset1 != 0 {
		t.Fatalf("expected partition 1 offset 0, got %d", offset1)
	}
}

func TestTopic_ReadFromPartition(t *testing.T) {
	dir := tempDir(t)
	topic := &tinykafka.Topic{Name: "orders"}
	for i := 0; i < 2; i++ {
		topic.Partitions = append(topic.Partitions, &tinykafka.Partition{
			Log: &tinykafka.Log{Filename: filepath.Join(dir, "orders-"+string(rune('0'+i))+".log")},
		})
	}
	if err := topic.Open(); err != nil {
		t.Fatal(err)
	}
	defer topic.Close()

	topic.AppendMessage(0, []byte("p0-data"))
	topic.AppendMessage(1, []byte("p1-data"))

	msg0, _ := topic.ReadMessage(0, 0)
	msg1, _ := topic.ReadMessage(1, 0)

	if !bytes.Equal(msg0, []byte("p0-data")) {
		t.Fatalf("expected 'p0-data', got %q", msg0)
	}
	if !bytes.Equal(msg1, []byte("p1-data")) {
		t.Fatalf("expected 'p1-data', got %q", msg1)
	}
}

func TestTopic_OpenAndClose(t *testing.T) {
	dir := tempDir(t)
	topic := &tinykafka.Topic{Name: "test"}
	for i := 0; i < 2; i++ {
		topic.Partitions = append(topic.Partitions, &tinykafka.Partition{
			Log: &tinykafka.Log{Filename: filepath.Join(dir, "test-"+string(rune('0'+i))+".log")},
		})
	}

	if err := topic.Open(); err != nil {
		t.Fatal(err)
	}
	if err := topic.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTopic_PartitionsAreIsolated(t *testing.T) {
	dir := tempDir(t)
	topic := &tinykafka.Topic{Name: "test"}
	for i := 0; i < 2; i++ {
		topic.Partitions = append(topic.Partitions, &tinykafka.Partition{
			Log: &tinykafka.Log{Filename: filepath.Join(dir, "test-"+string(rune('0'+i))+".log")},
		})
	}
	if err := topic.Open(); err != nil {
		t.Fatal(err)
	}
	defer topic.Close()

	topic.AppendMessage(0, []byte("only-in-p0"))
	topic.AppendMessage(1, []byte("only-in-p1"))

	msg0, _ := topic.ReadMessage(0, 0)
	msg1, _ := topic.ReadMessage(1, 0)

	if bytes.Equal(msg0, msg1) {
		t.Fatal("partitions should be isolated")
	}
	if !bytes.Equal(msg0, []byte("only-in-p0")) {
		t.Fatalf("expected 'only-in-p0', got %q", msg0)
	}
	if !bytes.Equal(msg1, []byte("only-in-p1")) {
		t.Fatalf("expected 'only-in-p1', got %q", msg1)
	}
}
