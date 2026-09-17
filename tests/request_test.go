package tinykafka_test

import (
	"bytes"
	"testing"

	"tinykafka-go/tinykafka"
)

func TestEncodeDecodeProduceRequest(t *testing.T) {
	req := tinykafka.ProduceRequest{
		Topic:       "orders",
		PartitionId: 0,
		Message:     []byte("hello-world"),
	}

	data, err := tinykafka.EncodeProduceRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	got, err := tinykafka.DecodeProduceRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.Topic != req.Topic {
		t.Fatalf("topic: expected %q, got %q", req.Topic, got.Topic)
	}
	if got.PartitionId != req.PartitionId {
		t.Fatalf("partition: expected %d, got %d", req.PartitionId, got.PartitionId)
	}
	if !bytes.Equal(got.Message, req.Message) {
		t.Fatalf("message: expected %q, got %q", req.Message, got.Message)
	}
}

func TestEncodeDecodeProduceRequestEmptyMessage(t *testing.T) {
	req := tinykafka.ProduceRequest{
		Topic:       "test",
		PartitionId: 1,
		Message:     []byte{},
	}

	data, err := tinykafka.EncodeProduceRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	got, err := tinykafka.DecodeProduceRequest(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Message) != 0 {
		t.Fatalf("expected empty message, got %d bytes", len(got.Message))
	}
}

func TestEncodeDecodeFetchRequest(t *testing.T) {
	req := tinykafka.FetchRequest{
		Topic:       "logs",
		PartitionId: 2,
		Offset:      1024,
	}

	data, err := tinykafka.EncodeFetchRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	got, err := tinykafka.DecodeFetchRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.Topic != req.Topic {
		t.Fatalf("topic: expected %q, got %q", req.Topic, got.Topic)
	}
	if got.PartitionId != req.PartitionId {
		t.Fatalf("partition: expected %d, got %d", req.PartitionId, got.PartitionId)
	}
	if got.Offset != req.Offset {
		t.Fatalf("offset: expected %d, got %d", req.Offset, got.Offset)
	}
}

func TestEncodeDecodeCreateTopicRequest(t *testing.T) {
	req := tinykafka.CreateTopicRequest{
		Topic:          "new-topic",
		PartitionCount: 4,
	}

	data, err := tinykafka.EncodeCreateTopicRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	got, err := tinykafka.DecodeCreateTopicRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.Topic != req.Topic {
		t.Fatalf("topic: expected %q, got %q", req.Topic, got.Topic)
	}
	if got.PartitionCount != req.PartitionCount {
		t.Fatalf("partitionCount: expected %d, got %d", req.PartitionCount, got.PartitionCount)
	}
}

func TestEncodeDecodeCreateTopicRequestZeroPartitions(t *testing.T) {
	req := tinykafka.CreateTopicRequest{
		Topic:          "empty",
		PartitionCount: 0,
	}

	data, err := tinykafka.EncodeCreateTopicRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	got, err := tinykafka.DecodeCreateTopicRequest(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.PartitionCount != 0 {
		t.Fatalf("expected 0 partitions, got %d", got.PartitionCount)
	}
}

func TestEncodeListTopicsRequest(t *testing.T) {
	data, err := tinykafka.EncodeListTopicsRequest()
	if err != nil {
		t.Fatal(err)
	}

	if len(data) != 5 {
		t.Fatalf("expected 5 bytes, got %d", len(data))
	}
}

func TestDecodeListTopicsRequestInvalidLength(t *testing.T) {
	data := []byte{0, 0, 0, 5, byte(tinykafka.ListTopicsRequestType), 0xFF}
	err := tinykafka.DecodeListTopicsRequest(data)
	if err == nil {
		t.Fatal("expected error for invalid length")
	}
}

func TestEncodeDecodeRequestEnvelope(t *testing.T) {
	body, _ := tinykafka.EncodeProduceRequest(tinykafka.ProduceRequest{
		Topic:       "t",
		PartitionId: 0,
		Message:     []byte("m"),
	})

	data, err := tinykafka.EncodeRequest(tinykafka.ProduceRequestType, body)
	if err != nil {
		t.Fatal(err)
	}

	reqType, reqBody, err := tinykafka.DecodeRequest(data)
	if err != nil {
		t.Fatal(err)
	}
	if reqType != tinykafka.ProduceRequestType {
		t.Fatalf("expected ProduceRequestType, got %d", reqType)
	}
	if !bytes.Equal(reqBody, body) {
		t.Fatal("body mismatch")
	}
}

func TestDecodeRequestTooShort(t *testing.T) {
	_, _, err := tinykafka.DecodeRequest([]byte{0, 0, 0})
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestDecodeRequestInvalidLength(t *testing.T) {
	data := []byte{0, 0, 0, 10, byte(tinykafka.ProduceRequestType)}
	_, _, err := tinykafka.DecodeRequest(data)
	if err == nil {
		t.Fatal("expected error for invalid length")
	}
}

func TestDecodeProduceRequestTooShort(t *testing.T) {
	_, err := tinykafka.DecodeProduceRequest([]byte{0, 1})
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestDecodeFetchRequestTooShort(t *testing.T) {
	_, err := tinykafka.DecodeFetchRequest([]byte{0, 1})
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestDecodeCreateTopicRequestTooShort(t *testing.T) {
	_, err := tinykafka.DecodeCreateTopicRequest([]byte{0, 1})
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}
