# MicroKafka

A minimal Kafka-style message broker written in Go, focused on the core produce/fetch path with performance optimizations.

## What it does

- **Topics** with configurable partition counts
- **Produce** messages to a topic/partition, get back an offset
- **Fetch** messages by topic/partition/offset
- **Persistence** — topics survive broker restarts
- **Concurrent** — multiple clients can produce/fetch simultaneously
- **TCP** with a custom binary wire protocol

## Wire protocol

```
[4 bytes: length][1 byte: type][body]

Request types:
  0 = CreateTopic
  1 = ListTopics
  2 = Produce
  3 = Fetch

All integers are little-endian.
```

## Performance optimizations

| Technique | What it does |
|-----------|-------------|
| `bufio` on TCP | Batches reads/writes into fewer syscalls |
| Direct response write | Skips intermediate buffer on server response |
| Hand-written encoding | No `binary.Write` reflection, manual byte packing |

### Benchmark results

```
  Producer Round-Trip Latency (single goroutine)
  Size     Msg/s      Avg(ms)    P99(ms)
  100B     44382      0.023      1.003
  1KB      38046      0.026      1.002
  10KB     17771      0.056      1.017
  100KB    5176       0.193      1.495

  Producer Throughput (1KB messages)
  Goroutines   Msg/s      Avg(ms)    P99(ms)
  1            37414      0.027      1.000
  4            65726      0.061      1.007
  8            65856      0.119      1.108
```

## Project structure

```
tinykafka/
  broker.go      — topic/partition management, persistence
  partition.go   — partition wrapper around Log
  log.go         — append-only message log with file I/O
  server.go      — TCP server, request handling
  request.go     — encode/decode for all request types
  response.go    — encode/decode for all response types
  router.go      — partition routing
  worker.go      — produce/fetch operations
  logger.go      — leveled logging
  metrics.go     — atomic counters

tests/
  87 tests covering broker, log, partition, topic, router,
  worker, request/response, server integration, metrics, logger
  benchmarks 
```



## What it is NOT

This is not a production Kafka replacement. It's a minimal broker for learning and benchmarking. No replication, no consumer groups, no transactions, no compression, no authentication.
