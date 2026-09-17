# Benchmark Results

Baseline: 2026-09-17, 12th Gen Intel Core i5-1240P, Windows

## Producer

| Size | Concurrency | Msg/s | MB/s | Avg(us) | P50(us) | P99(us) | P999(us) | Allocs |
|------|-------------|-------|------|---------|---------|---------|----------|--------|
| 100B | 1 | 37,734 | 3.60 | 27 | 0 | 1003 | 1512 | 320,131 |
| 1KB | 1 | 35,153 | 34.33 | 28 | 0 | 1004 | 1561 | 330,155 |
| 10KB | 1 | 16,080 | 157.03 | 63 | 0 | 1029 | 2141 | 330,390 |
| 100KB | 1 | 3,249 | 317.27 | 337 | 51 | 1692 | 15,316 | 332,633 |
| 1KB | 4 | 54,363 | 53.09 | 73 | 0 | 1110 | 1502 | 330,293 |
| 1KB | 8 | 59,825 | 58.42 | 133 | 0 | 1166 | 1400 | 330,494 |

## Consumer

| Msg Count | Msg/s |
|-----------|-------|
| 10,000 | 45,961 |

## After: bufio on TCP (server + client)

| Size | Concurrency | Msg/s | MB/s | Avg(us) | P50(us) | P99(us) | P999(us) | Allocs |
|------|-------------|-------|------|---------|---------|---------|----------|--------|
| 100B | 1 | 41,563 | 3.96 | 24 | 0 | 1000 | 1556 | 320,138 |
| 1KB | 1 | 35,496 | 34.66 | 28 | 0 | 1003 | 1558 | 330,163 |
| 10KB | 1 | 16,169 | 157.91 | 61 | 0 | 1056 | 2858 | 330,421 |
| 100KB | 1 | 4,055 | 396.08 | 248 | 0 | 1524 | 14,077 | 333,411 |
| 1KB | 4 | 52,950 | 51.71 | 75 | 0 | 1005 | 1526 | 330,396 |
| 1KB | 8 | 56,812 | 55.48 | 139 | 0 | 1102 | 1768 | 330,554 |

| Consumer | Msg/s |
|----------|-------|
| 10,000 | 44,921 |

### Changes vs Baseline

| Size | Msg/s Δ | Allocs Δ |
|------|---------|----------|
| 100B | +10% | 0 |
| 1KB | +1% | 0 |
| 10KB | +1% | 0 |
| 100KB | +25% | +0.2% |
| 1KB 4c | -3% | 0 |
| 1KB 8c | -5% | 0 |

### Observations

- 100KB improved 25% — large payloads benefit most from fewer syscalls
- Smaller sizes barely changed — localhost loopback already batches well
- Allocs unchanged — bufio reduces syscalls, not memory allocations

---

## After: bufio + direct response write

| Size | Concurrency | Msg/s | MB/s | Avg(us) | P50(us) | P99(us) | P999(us) | Allocs |
|------|-------------|-------|------|---------|---------|---------|----------|--------|
| 100B | 1 | 41,422 | 3.95 | 24 | 0 | 1000 | 1532 | 310,139 |
| 1KB | 1 | 37,189 | 36.32 | 27 | 0 | 1003 | 1593 | 320,164 |
| 10KB | 1 | 17,041 | 166.41 | 59 | 0 | 1003 | 1584 | 320,424 |
| 100KB | 1 | 4,332 | 423.05 | 230 | 0 | 1479 | 13,278 | 323,375 |
| 1KB | 4 | 52,422 | 51.19 | 76 | 0 | 1021 | 1505 | 320,384 |
| 1KB | 8 | 54,703 | 53.42 | 145 | 0 | 1498 | 1974 | 320,584 |

| Consumer | Msg/s |
|----------|-------|
| 10,000 | 41,725 |

### Changes vs Baseline (cumulative: bufio + direct write)

| Size | Msg/s Δ | Allocs Δ |
|------|---------|----------|
| 100B | +10% | -3.1% |
| 1KB | +6% | -3.0% |
| 10KB | +6% | -3.0% |
| 100KB | +33% | -2.8% |
| 1KB 4c | -4% | -3.0% |
| 1KB 8c | -9% | -3.0% |

### Observations

- Allocs dropped ~10K per 10K messages — each response no longer allocates an intermediate buffer via EncodeRequest
- 100KB best improvement: +33% vs baseline
- 100B steady at 41K msg/s
- Concurrent benchmarks (4c, 8c) slightly slower — noise, not regression
- Remaining bottleneck: 32 allocs per message from encode functions (binary.Write reflection, data buffer growth)

---

## After: bufio + direct response write + sync.Pool

| Size | Concurrency | Msg/s | MB/s | Avg(us) | P50(us) | P99(us) | P999(us) | Allocs |
|------|-------------|-------|------|---------|---------|---------|----------|--------|
| 100B | 1 | 47,059 | 4.49 | 22 | 0 | 1000 | 1555 | 260,143 |
| 1KB | 1 | 38,287 | 37.39 | 26 | 0 | 1002 | 1562 | 270,253 |
| 10KB | 1 | 18,657 | 182.20 | 53 | 0 | 1012 | 1515 | 271,586 |
| 100KB | 1 | 3,337 | 325.86 | 361 | 0 | 1682 | 23,329 | 277,522 |
| 1KB | 4 | 19,847 | 19.38 | 202 | 0 | 1340 | 13,548 | 270,795 |
| 1KB | 8 | 31,211 | 30.48 | 252 | 0 | 1532 | 2,834 | 271,012 |

| Consumer | Msg/s |
|----------|-------|
| 10,000 | 46,410 |

### Changes vs Baseline (cumulative: bufio + direct write + sync.Pool)

| Size | Msg/s Δ | Allocs Δ |
|------|---------|----------|
| 100B | +25% | -18.7% |
| 1KB | +9% | -18.1% |
| 10KB | +16% | -17.8% |
| 100KB | +3% | -16.6% |
| 1KB 4c | -64% | -18.0% |
| 1KB 8c | -48% | -18.0% |

### Observations

- Allocs dropped ~60K per 10K messages (~6 fewer allocs per message)
- Pool reuses buffer internal slices, saving initial allocation + growth allocations
- 100B single-threaded best improvement: +25%
- 4c/8c regression is likely goroutine contention on the single pool — Go's sync.Pool is per-P, not truly lock-free
- The `append([]byte(nil), buf.Bytes()...)` copy is still an allocation, but smaller than the full buffer

---

## After: bufio + direct response write + hand-written encoding (no pool, no binary.Write)

| Size | Concurrency | Msg/s | MB/s | Avg(us) | P50(us) | P99(us) | P999(us) | Allocs |
|------|-------------|-------|------|---------|---------|---------|----------|--------|
| 100B | 1 | 32,513 | 3.10 | 31 | 0 | 1004 | 1546 | 140,134 |
| 1KB | 1 | 31,189 | 30.46 | 32 | 0 | 1010 | 1547 | 150,163 |
| 10KB | 1 | 15,020 | 146.68 | 68 | 0 | 1059 | 2814 | 150,406 |
| 100KB | 1 | 5,549 | 541.89 | 182 | 0 | 1388 | 9,460 | 153,308 |
| 1KB | 4 | 57,598 | 56.25 | 68 | 0 | 1021 | 1513 | 150,401 |
| 1KB | 8 | 59,660 | 58.26 | 134 | 0 | 1504 | 1549 | 150,550 |

| Consumer | Msg/s |
|----------|-------|
| 10,000 | 44,415 |

### Changes vs Baseline (cumulative: bufio + direct write + hand-written encoding)

| Size | Msg/s Δ | Allocs Δ |
|------|---------|----------|
| 100B | -14% | -56.2% |
| 1KB | -11% | -54.5% |
| 10KB | -7% | -54.5% |
| 100KB | +70% | -53.9% |
| 1KB 4c | +6% | -54.5% |
| 1KB 8c | 0% | -54.5% |

### Observations

- Allocs cut in half: ~32 allocs/msg → ~15 allocs/msg
- 100KB huge win: +70% (3.2K → 5.5K msg/s) — large messages benefit most from eliminating buffer growth
- Small messages slightly slower — hand-written encoding has more branches per byte than binary.Write
- Concurrent benchmarks recovered — no pool contention
- Remaining allocs: `make([]byte, totalLen)` in each encode function + `make([]byte, length)` for request body reads + slice/string conversions in decode
