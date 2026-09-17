package tinykafka_test

import (
	"sync"
	"testing"

	"tinykafka-go/tinykafka"
)

func TestDefaultMetrics(t *testing.T) {
	m := tinykafka.DefaultMetrics()
	if m == nil {
		t.Fatal("DefaultMetrics returned nil")
	}
	snap := m.Snapshot()
	if snap.TotalRequests != 0 {
		t.Fatalf("expected 0 total requests, got %d", snap.TotalRequests)
	}
}

func TestRecordRequest_CreateTopic(t *testing.T) {
	m := &tinykafka.Metrics{}
	m.RecordRequest(tinykafka.CreateTopicRequestType)
	snap := m.Snapshot()
	if snap.TotalRequests != 1 {
		t.Fatalf("expected 1 total, got %d", snap.TotalRequests)
	}
	if snap.CreateTopicRequests != 1 {
		t.Fatalf("expected 1 create topic, got %d", snap.CreateTopicRequests)
	}
	if snap.ListTopicsRequests != 0 {
		t.Fatalf("expected 0 list topics, got %d", snap.ListTopicsRequests)
	}
}

func TestRecordRequest_ListTopics(t *testing.T) {
	m := &tinykafka.Metrics{}
	m.RecordRequest(tinykafka.ListTopicsRequestType)
	snap := m.Snapshot()
	if snap.TotalRequests != 1 {
		t.Fatalf("expected 1 total, got %d", snap.TotalRequests)
	}
	if snap.ListTopicsRequests != 1 {
		t.Fatalf("expected 1 list topics, got %d", snap.ListTopicsRequests)
	}
}

func TestRecordRequest_Produce(t *testing.T) {
	m := &tinykafka.Metrics{}
	m.RecordRequest(tinykafka.ProduceRequestType)
	snap := m.Snapshot()
	if snap.TotalRequests != 1 {
		t.Fatalf("expected 1 total, got %d", snap.TotalRequests)
	}
	if snap.ProduceRequests != 1 {
		t.Fatalf("expected 1 produce, got %d", snap.ProduceRequests)
	}
}

func TestRecordRequest_Fetch(t *testing.T) {
	m := &tinykafka.Metrics{}
	m.RecordRequest(tinykafka.FetchRequestType)
	snap := m.Snapshot()
	if snap.TotalRequests != 1 {
		t.Fatalf("expected 1 total, got %d", snap.TotalRequests)
	}
	if snap.FetchRequests != 1 {
		t.Fatalf("expected 1 fetch, got %d", snap.FetchRequests)
	}
}

func TestRecordBytesRead(t *testing.T) {
	m := &tinykafka.Metrics{}
	m.RecordBytesRead(100)
	m.RecordBytesRead(200)
	snap := m.Snapshot()
	if snap.TotalBytesRead != 300 {
		t.Fatalf("expected 300 bytes read, got %d", snap.TotalBytesRead)
	}
}

func TestRecordBytesWritten(t *testing.T) {
	m := &tinykafka.Metrics{}
	m.RecordBytesWritten(50)
	m.RecordBytesWritten(150)
	snap := m.Snapshot()
	if snap.TotalBytesWritten != 200 {
		t.Fatalf("expected 200 bytes written, got %d", snap.TotalBytesWritten)
	}
}

func TestRecordError(t *testing.T) {
	m := &tinykafka.Metrics{}
	m.RecordError()
	m.RecordError()
	m.RecordError()
	snap := m.Snapshot()
	if snap.TotalErrors != 3 {
		t.Fatalf("expected 3 errors, got %d", snap.TotalErrors)
	}
}

func TestMetrics_MultipleRequests(t *testing.T) {
	m := &tinykafka.Metrics{}
	m.RecordRequest(tinykafka.ProduceRequestType)
	m.RecordRequest(tinykafka.ProduceRequestType)
	m.RecordRequest(tinykafka.FetchRequestType)
	m.RecordRequest(tinykafka.CreateTopicRequestType)
	m.RecordRequest(tinykafka.ListTopicsRequestType)
	snap := m.Snapshot()
	if snap.TotalRequests != 5 {
		t.Fatalf("expected 5 total, got %d", snap.TotalRequests)
	}
	if snap.ProduceRequests != 2 {
		t.Fatalf("expected 2 produce, got %d", snap.ProduceRequests)
	}
	if snap.FetchRequests != 1 {
		t.Fatalf("expected 1 fetch, got %d", snap.FetchRequests)
	}
	if snap.CreateTopicRequests != 1 {
		t.Fatalf("expected 1 create topic, got %d", snap.CreateTopicRequests)
	}
	if snap.ListTopicsRequests != 1 {
		t.Fatalf("expected 1 list topics, got %d", snap.ListTopicsRequests)
	}
}

func TestMetrics_ConcurrentRecordRequest(t *testing.T) {
	m := &tinykafka.Metrics{}
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.RecordRequest(tinykafka.ProduceRequestType)
		}()
	}
	wg.Wait()
	snap := m.Snapshot()
	if snap.TotalRequests != 1000 {
		t.Fatalf("expected 1000 total, got %d", snap.TotalRequests)
	}
	if snap.ProduceRequests != 1000 {
		t.Fatalf("expected 1000 produce, got %d", snap.ProduceRequests)
	}
}

func TestMetrics_ConcurrentMixed(t *testing.T) {
	m := &tinykafka.Metrics{}
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			m.RecordRequest(tinykafka.ProduceRequestType)
		}()
		go func() {
			defer wg.Done()
			m.RecordBytesRead(1)
		}()
		go func() {
			defer wg.Done()
			m.RecordError()
		}()
	}
	wg.Wait()
	snap := m.Snapshot()
	if snap.TotalRequests != 500 {
		t.Fatalf("expected 500 total, got %d", snap.TotalRequests)
	}
	if snap.TotalBytesRead != 500 {
		t.Fatalf("expected 500 bytes read, got %d", snap.TotalBytesRead)
	}
	if snap.TotalErrors != 500 {
		t.Fatalf("expected 500 errors, got %d", snap.TotalErrors)
	}
}

func TestMetrics_SnapshotIsCopy(t *testing.T) {
	m := &tinykafka.Metrics{}
	m.RecordRequest(tinykafka.ProduceRequestType)
	snap1 := m.Snapshot()
	m.RecordRequest(tinykafka.ProduceRequestType)
	snap2 := m.Snapshot()
	if snap1.TotalRequests != 1 {
		t.Fatalf("snap1 should have 1 request, got %d", snap1.TotalRequests)
	}
	if snap2.TotalRequests != 2 {
		t.Fatalf("snap2 should have 2 requests, got %d", snap2.TotalRequests)
	}
}
