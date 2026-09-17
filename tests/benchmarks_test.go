package tinykafka_test

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

type benchResult struct {
	Messages    int
	Size        int
	Concurrency int
	TotalTime   time.Duration
	Latencies   []float64
}

func (r benchResult) Avg() float64 {
	var sum float64
	for _, l := range r.Latencies {
		sum += l
	}
	return sum / float64(len(r.Latencies))
}

func (r benchResult) ThroughputMsg() float64 {
	return float64(r.Messages) / r.TotalTime.Seconds()
}

func (r benchResult) ThroughputMB() float64 {
	return r.ThroughputMsg() * float64(r.Size) / (1024 * 1024)
}

func (r benchResult) SortedLatencies() []float64 {
	s := make([]float64, len(r.Latencies))
	copy(s, r.Latencies)
	sort.Float64s(s)
	return s
}

func runProducerBench(t testing.TB, messages, size, concurrency int) benchResult {
	t.Helper()
	addr, cleanup := startTestServer(t, map[string]int{"bench": 1})
	defer cleanup()

	msg := make([]byte, size)
	for i := range msg {
		msg[i] = 0x42
	}

	var mu sync.Mutex
	allLatencies := make([]float64, 0, messages)
	var total atomic.Int64

	var wg sync.WaitGroup
	messagesPerWorker := messages / concurrency

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bc, err := dialBuffered(addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer bc.close()

			var localLatencies []float64
			for j := 0; j < messagesPerWorker; j++ {
				t0 := time.Now()
				if _, err := produceViaTCP(bc, "bench", 0, msg); err != nil {
					t.Error(err)
					return
				}
				lat := float64(time.Since(t0).Nanoseconds()) / 1000.0
				localLatencies = append(localLatencies, lat)
				total.Add(1)
			}

			mu.Lock()
			allLatencies = append(allLatencies, localLatencies...)
			mu.Unlock()
		}()
	}

	wg.Wait()
	totalTime := time.Since(start)

	return benchResult{
		Messages:    int(total.Load()),
		Size:        size,
		Concurrency: concurrency,
		TotalTime:   totalTime,
		Latencies:   allLatencies,
	}
}

func printBenchResult(t testing.TB, r benchResult) {
	t.Helper()
	sorted := r.SortedLatencies()
	t.Logf("  %d msgs, %s, %d goroutines: %.0f msg/s, %.1f MB/s, avg=%.3fms p50=%.3fms p99=%.3fms p99.9=%.3fms",
		r.Messages,
		humanSize(r.Size),
		r.Concurrency,
		r.ThroughputMsg(),
		r.ThroughputMB(),
		r.Avg()/1000,
		percentile(sorted, 50)/1000,
		percentile(sorted, 99)/1000,
		percentile(sorted, 99.9)/1000,
	)
}

func humanSize(b int) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%dMB", b/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%dKB", b/1024)
	default:
		return fmt.Sprintf("%dB", b)
	}
}

func BenchmarkProducer_100B_1(t *testing.B) {
	for i := 0; i < t.N; i++ {
		r := runProducerBench(t, 10000, 100, 1)
		printBenchResult(t, r)
	}
}

func BenchmarkProducer_1KB_1(t *testing.B) {
	for i := 0; i < t.N; i++ {
		r := runProducerBench(t, 10000, 1024, 1)
		printBenchResult(t, r)
	}
}

func BenchmarkProducer_10KB_1(t *testing.B) {
	for i := 0; i < t.N; i++ {
		r := runProducerBench(t, 10000, 10240, 1)
		printBenchResult(t, r)
	}
}

func BenchmarkProducer_100KB_1(t *testing.B) {
	for i := 0; i < t.N; i++ {
		r := runProducerBench(t, 10000, 102400, 1)
		printBenchResult(t, r)
	}
}

func BenchmarkProducer_1KB_4(t *testing.B) {
	for i := 0; i < t.N; i++ {
		r := runProducerBench(t, 10000, 1024, 4)
		printBenchResult(t, r)
	}
}

func BenchmarkProducer_1KB_8(t *testing.B) {
	for i := 0; i < t.N; i++ {
		r := runProducerBench(t, 10000, 1024, 8)
		printBenchResult(t, r)
	}
}

func BenchmarkConsumer_10K(t *testing.B) {
	for i := 0; i < t.N; i++ {
		addr, cleanup := startTestServer(t, map[string]int{"bench": 1})

		bc, err := dialBuffered(addr)
		if err != nil {
			t.Fatal(err)
		}

		msgCount := 10000
		msg := make([]byte, 100)
		for j := range msg {
			msg[j] = 0x42
		}

		offsets := make([]int64, msgCount)
		for j := 0; j < msgCount; j++ {
			offset, err := produceViaTCP(bc, "bench", 0, msg)
			if err != nil {
				t.Fatal(err)
			}
			offsets[j] = offset
		}

		t.ResetTimer()

		start := time.Now()
		for j := 0; j < msgCount; j++ {
			if _, err := fetchViaTCP(bc, "bench", 0, offsets[j]); err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(start)

		t.StopTimer()

		t.Logf("fetched %d messages in %.3fs (%.0f msg/s)",
			msgCount, elapsed.Seconds(), float64(msgCount)/elapsed.Seconds())

		bc.close()
		cleanup()
	}
}

func BenchmarkRoundTrip_PrintTable(b *testing.B) {
	b.Run("latency_by_size", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sizes := []int{100, 1024, 10240, 102400}
			b.Logf("  %-8s %-10s %-10s %-10s", "Size", "Msg/s", "Avg(ms)", "P99(ms)")
			for _, size := range sizes {
				r := runProducerBench(b, 10000, size, 1)
				sorted := r.SortedLatencies()
				b.Logf("  %-8s %-10.0f %-10.3f %-10.3f",
					humanSize(size),
					r.ThroughputMsg(),
					r.Avg()/1000,
					percentile(sorted, 99)/1000,
				)
			}
		}
	})

	b.Run("throughput_by_concurrency", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.Logf("  %-12s %-10s %-10s %-10s", "Goroutines", "Msg/s", "Avg(ms)", "P99(ms)")
			for _, c := range []int{1, 4, 8} {
				r := runProducerBench(b, 10000, 1024, c)
				sorted := r.SortedLatencies()
				b.Logf("  %-12d %-10.0f %-10.3f %-10.3f",
					c,
					r.ThroughputMsg(),
					r.Avg()/1000,
					percentile(sorted, 99)/1000,
				)
			}
		}
	})
}
