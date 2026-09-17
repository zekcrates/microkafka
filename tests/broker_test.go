package tinykafka_test

import (
	"bytes"
	"sort"
	"testing"

	"tinykafka-go/tinykafka"
)

func TestBroker_CreateTopic(t *testing.T) {
	b := setupBroker(t)

	if err := b.CreateTopic("orders", 3); err != nil {
		t.Fatal(err)
	}
	if !b.HasTopic("orders") {
		t.Fatal("topic 'orders' should exist")
	}
}

func TestBroker_GetTopic(t *testing.T) {
	b := setupBroker(t)
	b.CreateTopic("orders", 2)

	topic, err := b.GetTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	if topic.Name != "orders" {
		t.Fatalf("expected name 'orders', got %q", topic.Name)
	}
	if len(topic.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(topic.Partitions))
	}
}

func TestBroker_GetTopicNotFound(t *testing.T) {
	b := setupBroker(t)

	_, err := b.GetTopic("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent topic")
	}
}

func TestBroker_HasTopic(t *testing.T) {
	b := setupBroker(t)

	if b.HasTopic("foo") {
		t.Fatal("should not have 'foo' yet")
	}
	b.CreateTopic("foo", 1)
	if !b.HasTopic("foo") {
		t.Fatal("should have 'foo' after creation")
	}
}

func TestBroker_ListTopics(t *testing.T) {
	b := setupBroker(t)

	b.CreateTopic("alpha", 1)
	b.CreateTopic("beta", 1)
	b.CreateTopic("gamma", 1)

	topics := b.ListTopics()
	if len(topics) != 3 {
		t.Fatalf("expected 3 topics, got %d", len(topics))
	}
}

func TestBroker_ListTopicsEmpty(t *testing.T) {
	b := setupBroker(t)

	topics := b.ListTopics()
	if len(topics) != 0 {
		t.Fatalf("expected 0 topics, got %d", len(topics))
	}
}

func TestBroker_OpenAndClose(t *testing.T) {
	b := setupBroker(t)
	b.CreateTopic("t1", 1)
	b.CreateTopic("t2", 1)

	if err := b.Open(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBroker_EndToEnd(t *testing.T) {
	b := setupBroker(t)
	b.CreateTopic("orders", 1)
	if err := b.Open(); err != nil {
		t.Fatal(err)
	}

	topic, _ := b.GetTopic("orders")
	offset, err := topic.AppendMessage(0, []byte("e2e-message"))
	if err != nil {
		t.Fatal(err)
	}

	msg, err := topic.ReadMessage(0, offset)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg, []byte("e2e-message")) {
		t.Fatalf("expected 'e2e-message', got %q", msg)
	}
}

func TestBroker_SaveTopicsToFile(t *testing.T) {
	dir := t.TempDir()
	b := tinykafka.NewBroker(dir)

	b.CreateTopic("orders", 3)
	b.CreateTopic("logs", 1)

	if err := b.SaveTopicsToFile(); err != nil {
		t.Fatal(err)
	}

	b2 := tinykafka.NewBroker(dir)
	if err := b2.LoadTopics(); err != nil {
		t.Fatal(err)
	}

	if !b2.HasTopic("orders") {
		t.Fatal("expected 'orders' after load")
	}
	if !b2.HasTopic("logs") {
		t.Fatal("expected 'logs' after load")
	}

	topic, err := b2.GetTopic("orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(topic.Partitions) != 3 {
		t.Fatalf("expected 3 partitions for 'orders', got %d", len(topic.Partitions))
	}

	topic, err = b2.GetTopic("logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(topic.Partitions) != 1 {
		t.Fatalf("expected 1 partition for 'logs', got %d", len(topic.Partitions))
	}
}

func TestBroker_LoadTopics_NoFile(t *testing.T) {
	dir := t.TempDir()
	b := tinykafka.NewBroker(dir)

	if err := b.LoadTopics(); err != nil {
		t.Fatal("loading from nonexistent file should not error")
	}

	if len(b.ListTopics()) != 0 {
		t.Fatal("expected 0 topics")
	}
}

func TestBroker_LoadTopics_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	b := tinykafka.NewBroker(dir)

	b.SaveTopicsToFile()

	b2 := tinykafka.NewBroker(dir)
	if err := b2.LoadTopics(); err != nil {
		t.Fatal(err)
	}

	if len(b2.ListTopics()) != 0 {
		t.Fatal("expected 0 topics from empty file")
	}
}

func TestBroker_PersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	b1 := tinykafka.NewBroker(dir)
	b1.CreateTopic("persist", 2)
	b1.Open()
	b1.SaveTopicsToFile()
	b1.Close()

	b2 := tinykafka.NewBroker(dir)
	if err := b2.LoadTopics(); err != nil {
		t.Fatal(err)
	}
	if err := b2.Open(); err != nil {
		t.Fatal(err)
	}
	defer b2.Close()

	if !b2.HasTopic("persist") {
		t.Fatal("topic 'persist' should survive reopen")
	}

	topic, err := b2.GetTopic("persist")
	if err != nil {
		t.Fatal(err)
	}
	if len(topic.Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(topic.Partitions))
	}
}

func TestBroker_PersistenceWithData(t *testing.T) {
	dir := t.TempDir()

	b1 := tinykafka.NewBroker(dir)
	b1.CreateTopic("data", 1)
	b1.Open()

	topic, _ := b1.GetTopic("data")
	offset, err := topic.AppendMessage(0, []byte("persisted-message"))
	if err != nil {
		t.Fatal(err)
	}

	b1.SaveTopicsToFile()
	b1.Close()

	b2 := tinykafka.NewBroker(dir)
	b2.LoadTopics()
	b2.Open()
	defer b2.Close()

	topic2, err := b2.GetTopic("data")
	if err != nil {
		t.Fatal(err)
	}

	msg, err := topic2.ReadMessage(0, offset)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg, []byte("persisted-message")) {
		t.Fatalf("expected 'persisted-message', got %q", msg)
	}
}

func TestBroker_PersistenceManyTopics(t *testing.T) {
	dir := t.TempDir()

	b1 := tinykafka.NewBroker(dir)
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i, name := range names {
		b1.CreateTopic(name, i+1)
	}
	b1.SaveTopicsToFile()

	b2 := tinykafka.NewBroker(dir)
	b2.LoadTopics()

	got := b2.ListTopics()
	sort.Strings(got)
	sort.Strings(names)

	if len(got) != len(names) {
		t.Fatalf("expected %d topics, got %d", len(names), len(got))
	}
	for i := range got {
		if got[i] != names[i] {
			t.Fatalf("topic %d: expected %q, got %q", i, names[i], got[i])
		}
	}
}

func TestBroker_SaveCreatesFile(t *testing.T) {
	dir := t.TempDir()
	b := tinykafka.NewBroker(dir)
	b.CreateTopic("t", 1)

	if err := b.SaveTopicsToFile(); err != nil {
		t.Fatal(err)
	}

	b2 := tinykafka.NewBroker(dir)
	if err := b2.LoadTopics(); err != nil {
		t.Fatal(err)
	}
	if !b2.HasTopic("t") {
		t.Fatal("topic should be loadable after save")
	}
}
