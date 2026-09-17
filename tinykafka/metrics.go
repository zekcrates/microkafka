package tinykafka

import "sync/atomic"

type Metrics struct {
	TotalRequests     atomic.Int64
	TotalBytesRead    atomic.Int64
	TotalBytesWritten atomic.Int64
	TotalErrors       atomic.Int64

	CreateTopicRequests atomic.Int64
	ListTopicsRequests  atomic.Int64
	ProduceRequests     atomic.Int64
	FetchRequests       atomic.Int64
}

var defaultMetrics = &Metrics{}

func (m *Metrics) RecordRequest(reqType RequestType) {
	switch reqType {
	case CreateTopicRequestType:
		m.CreateTopicRequests.Add(1)
	case ListTopicsRequestType:
		m.ListTopicsRequests.Add(1)
	case ProduceRequestType:
		m.ProduceRequests.Add(1)
	case FetchRequestType:
		m.FetchRequests.Add(1)
	}
	m.TotalRequests.Add(1)
}

func (m *Metrics) RecordBytesRead(n int64) {
	m.TotalBytesRead.Add(n)
}

func (m *Metrics) RecordBytesWritten(n int64) {
	m.TotalBytesWritten.Add(n)
}

func (m *Metrics) RecordError() {
	m.TotalErrors.Add(1)
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		TotalRequests:      m.TotalRequests.Load(),
		TotalBytesRead:     m.TotalBytesRead.Load(),
		TotalBytesWritten:  m.TotalBytesWritten.Load(),
		TotalErrors:        m.TotalErrors.Load(),
		CreateTopicRequests: m.CreateTopicRequests.Load(),
		ListTopicsRequests:  m.ListTopicsRequests.Load(),
		ProduceRequests:     m.ProduceRequests.Load(),
		FetchRequests:       m.FetchRequests.Load(),
	}
}

type MetricsSnapshot struct {
	TotalRequests      int64
	TotalBytesRead     int64
	TotalBytesWritten  int64
	TotalErrors        int64
	CreateTopicRequests int64
	ListTopicsRequests  int64
	ProduceRequests     int64
	FetchRequests       int64
}

func DefaultMetrics() *Metrics {
	return defaultMetrics
}
