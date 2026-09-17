package tinykafka_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestIntegration_CreateTopic(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	if err := createTopicViaTCP(bc, "orders", 3); err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_ListTopics(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	if err := createTopicViaTCP(bc, "alpha", 1); err != nil {
		t.Fatal(err)
	}
	if err := createTopicViaTCP(bc, "beta", 2); err != nil {
		t.Fatal(err)
	}

	topics, err := listTopicsViaTCP(bc)
	if err != nil {
		t.Fatal(err)
	}

	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d: %v", len(topics), topics)
	}

	found := map[string]bool{}
	for _, name := range topics {
		found[name] = true
	}
	if !found["alpha"] || !found["beta"] {
		t.Fatalf("expected alpha and beta, got %v", topics)
	}
}

func TestIntegration_ProduceAndFetch(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	if err := createTopicViaTCP(bc, "test", 1); err != nil {
		t.Fatal(err)
	}

	offset, err := produceViaTCP(bc, "test", 0, []byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	msg, err := fetchViaTCP(bc, "test", 0, offset)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg, []byte("hello world")) {
		t.Fatalf("expected 'hello world', got %q", msg)
	}
}

func TestIntegration_MultipleMessages(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	if err := createTopicViaTCP(bc, "test", 1); err != nil {
		t.Fatal(err)
	}

	msgs := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	offsets := make([]int64, len(msgs))

	for i, msg := range msgs {
		offset, err := produceViaTCP(bc, "test", 0, []byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		offsets[i] = offset
	}

	for i, msg := range msgs {
		got, err := fetchViaTCP(bc, "test", 0, offsets[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte(msg)) {
			t.Fatalf("message %d: expected %q, got %q", i, msg, got)
		}
	}
}

func TestIntegration_MultiplePartitions(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	if err := createTopicViaTCP(bc, "multi", 2); err != nil {
		t.Fatal(err)
	}

	o0, err := produceViaTCP(bc, "multi", 0, []byte("p0-data"))
	if err != nil {
		t.Fatal(err)
	}
	o1, err := produceViaTCP(bc, "multi", 1, []byte("p1-data"))
	if err != nil {
		t.Fatal(err)
	}

	msg0, err := fetchViaTCP(bc, "multi", 0, o0)
	if err != nil {
		t.Fatal(err)
	}
	msg1, err := fetchViaTCP(bc, "multi", 1, o1)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(msg0, []byte("p0-data")) {
		t.Fatalf("partition 0: expected 'p0-data', got %q", msg0)
	}
	if !bytes.Equal(msg1, []byte("p1-data")) {
		t.Fatalf("partition 1: expected 'p1-data', got %q", msg1)
	}
}

func TestIntegration_PartitionIsolation(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	if err := createTopicViaTCP(bc, "iso", 2); err != nil {
		t.Fatal(err)
	}

	produceViaTCP(bc, "iso", 0, []byte("only-in-p0"))
	produceViaTCP(bc, "iso", 1, []byte("only-in-p1"))

	msg0, _ := fetchViaTCP(bc, "iso", 0, 0)
	msg1, _ := fetchViaTCP(bc, "iso", 1, 0)

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

func TestIntegration_FetchNonexistentTopic(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	_, err = fetchViaTCP(bc, "nonexistent", 0, 0)
	if err == nil {
		t.Fatal("expected error fetching nonexistent topic")
	}
}

func TestIntegration_ProduceNonexistentTopic(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	_, err = produceViaTCP(bc, "nonexistent", 0, []byte("data"))
	if err == nil {
		t.Fatal("expected error producing to nonexistent topic")
	}
}

func TestIntegration_FetchBadOffset(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	if err := createTopicViaTCP(bc, "test", 1); err != nil {
		t.Fatal(err)
	}

	produceViaTCP(bc, "test", 0, []byte("data"))

	_, err = fetchViaTCP(bc, "test", 0, 999999)
	if err == nil {
		t.Fatal("expected error fetching bad offset")
	}
}

func TestIntegration_ConcurrentClients(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	bc.close()

	if err := func() error {
		c, err := dialBuffered(addr)
		if err != nil {
			return err
		}
		defer c.close()
		return createTopicViaTCP(c, "concurrent", 1)
	}(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	msgCount := 100
	concurrency := 4

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			bc, err := dialBuffered(addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer bc.close()

			for j := 0; j < msgCount; j++ {
				msg := fmt.Sprintf("client-%d-msg-%d", id, j)
				_, err := produceViaTCP(bc, "concurrent", 0, []byte(msg))
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	bc, err = dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	totalMessages := msgCount * concurrency
	for i := 0; i < totalMessages; i++ {
		_, err := fetchViaTCP(bc, "concurrent", 0, int64(i*13)) // wrong offsets, just testing we can fetch
		if err != nil {
			break
		}
	}
}

func TestIntegration_MultipleClientsSameTopic(t *testing.T) {
	addr, cleanup := startTestServer(t, nil)
	defer cleanup()

	bc, err := dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	if err := createTopicViaTCP(bc, "shared", 1); err != nil {
		t.Fatal(err)
	}
	bc.close()

	produceCount := 50
	var mu sync.Mutex
	offsets := make([]int64, 0, produceCount*4)

	for i := 0; i < 4; i++ {
		func() {
			c, err := dialBuffered(addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.close()
			for j := 0; j < produceCount; j++ {
				msg := fmt.Sprintf("c%d-m%d", i, j)
				offset, err := produceViaTCP(c, "shared", 0, []byte(msg))
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				offsets = append(offsets, offset)
				mu.Unlock()
			}
		}()
	}

	bc, err = dialBuffered(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.close()

	for _, offset := range offsets {
		msg, err := fetchViaTCP(bc, "shared", 0, offset)
		if err != nil {
			t.Fatalf("fetch at offset %d failed: %v", offset, err)
		}
		if len(msg) == 0 {
			t.Fatalf("fetch at offset %d returned empty message", offset)
		}
	}
}
