# MicroKafka

A minimal Kafka-style message broker written in Go.

## Usage

```go
broker := tinykafka.NewBroker("/tmp/microkafka-data")
broker.CreateTopic("events", 3)
broker.Open()

router := tinykafka.NewRouter([]*tinykafka.Worker{tinykafka.NewWorker(broker)})
server := tinykafka.NewServer(router, broker)
server.Start("127.0.0.1:9092")
```

## What it is NOT

This is not a production Kafka replacement. It's a minimal broker for learning and benchmarking. No replication, no consumer groups, no transactions, no compression, no authentication.

