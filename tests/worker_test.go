package tinykafka_test

import (
	"bytes"
	"testing"
)

func TestWorker_AppendAndRead(t *testing.T) {
	w := setupWorker(t)
	w.Broker.CreateTopic("orders", 1)
	w.Broker.Open()

	offset, err := w.AppendMessage("orders", 0, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	msg, err := w.ReadMessage("orders", 0, offset)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg, []byte("hello")) {
		t.Fatalf("expected 'hello', got %q", msg)
	}
}

func TestWorker_AppendNonExistentTopic(t *testing.T) {
	w := setupWorker(t)

	_, err := w.AppendMessage("nonexistent", 0, []byte("data"))
	if err == nil {
		t.Fatal("expected error for nonexistent topic")
	}
}

func TestWorker_ReadNonExistentTopic(t *testing.T) {
	w := setupWorker(t)

	_, err := w.ReadMessage("nonexistent", 0, 0)
	if err == nil {
		t.Fatal("expected error for nonexistent topic")
	}
}

func TestWorker_EndToEndMultipleMessages(t *testing.T) {
	w := setupWorker(t)
	w.Broker.CreateTopic("logs", 1)
	w.Broker.Open()

	msgs := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	offsets := make([]int64, len(msgs))

	for i, msg := range msgs {
		offset, err := w.AppendMessage("logs", 0, []byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		offsets[i] = offset
	}

	for i, msg := range msgs {
		got, err := w.ReadMessage("logs", 0, offsets[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte(msg)) {
			t.Fatalf("message %d: expected %q, got %q", i, msg, got)
		}
	}
}

func TestWorker_CrossPartitionIsolation(t *testing.T) {
	w := setupWorker(t)
	w.Broker.CreateTopic("topic", 2)
	w.Broker.Open()

	w.AppendMessage("topic", 0, []byte("p0-data"))
	w.AppendMessage("topic", 1, []byte("p1-data"))

	msg0, _ := w.ReadMessage("topic", 0, 0)
	msg1, _ := w.ReadMessage("topic", 1, 0)

	if !bytes.Equal(msg0, []byte("p0-data")) {
		t.Fatalf("expected 'p0-data', got %q", msg0)
	}
	if !bytes.Equal(msg1, []byte("p1-data")) {
		t.Fatalf("expected 'p1-data', got %q", msg1)
	}
}

// func TestWorker_AutoCreateTopic(t *testing.T) {
// 	w := setupWorker(t)
// 	offset, err := w.AppendMessage("auto-topic", 0, []byte("data"))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	msg, _ := w.ReadMessage("auto-topic", 0, offset)
// 	if !bytes.Equal(msg, []byte("data")) {
// 		t.Fatal("auto-create topic failed")
// 	}
// }

// func TestWorker_ParallelAppend(t *testing.T) {
// 	w := setupWorker(t)
// 	w.Broker.CreateTopic("parallel", 4)
// 	// test concurrent writes to same/different partitions
// }
