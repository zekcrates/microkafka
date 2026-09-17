package tinykafka

import (
	"fmt"
	"io"
)

type RequestType uint8

const (
	CreateTopicRequestType RequestType = iota
	ListTopicsRequestType
	ProduceRequestType
	FetchRequestType
)

type CreateTopicRequest struct {
	Topic          string
	PartitionCount int32
}

type ListTopicsRequest struct{}

type ProduceRequest struct {
	Topic       string
	PartitionId int32
	Message     []byte
}

type FetchRequest struct {
	Topic       string
	PartitionId int32
	Offset      int64
}

func putUint32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func putInt32(b []byte, v int32) {
	putUint32(b, uint32(v))
}

func putInt64(b []byte, v int64) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
}

func getUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func getInt32(b []byte) int32 {
	return int32(getUint32(b))
}

func getInt64(b []byte) int64 {
	return int64(b[0]) | int64(b[1])<<8 | int64(b[2])<<16 | int64(b[3])<<24 |
		int64(b[4])<<32 | int64(b[5])<<40 | int64(b[6])<<48 | int64(b[7])<<56
}

func EncodeProduceRequest(req ProduceRequest) ([]byte, error) {
	topicLen := len(req.Topic)
	msgLen := len(req.Message)
	totalLen := 4 + topicLen + 4 + 4 + msgLen
	result := make([]byte, totalLen)
	off := 0
	putUint32(result[off:], uint32(topicLen))
	off += 4
	copy(result[off:], req.Topic)
	off += topicLen
	putInt32(result[off:], req.PartitionId)
	off += 4
	putUint32(result[off:], uint32(msgLen))
	off += 4
	copy(result[off:], req.Message)
	return result, nil
}

func DecodeProduceRequest(data []byte) (ProduceRequest, error) {
	if len(data) < 4 {
		return ProduceRequest{}, fmt.Errorf("produce request too short")
	}
	off := 0
	topicLen := int(getUint32(data[off:]))
	off += 4
	if len(data) < off+topicLen+4+4 {
		return ProduceRequest{}, fmt.Errorf("produce request too short")
	}
	topic := string(data[off : off+topicLen])
	off += topicLen
	partitionID := getInt32(data[off:])
	off += 4
	messageLen := int(getUint32(data[off:]))
	off += 4
	var message []byte
	if messageLen > 0 {
		if len(data) < off+messageLen {
			return ProduceRequest{}, io.ErrUnexpectedEOF
		}
		message = make([]byte, messageLen)
		copy(message, data[off:off+messageLen])
	}
	return ProduceRequest{
		Topic:       topic,
		PartitionId: partitionID,
		Message:     message,
	}, nil
}

func EncodeRequest(reqType RequestType, body []byte) ([]byte, error) {
	length := uint32(1 + len(body))
	result := make([]byte, 4+int(length))
	putUint32(result[:4], length)
	result[4] = byte(reqType)
	copy(result[5:], body)
	return result, nil
}

func DecodeRequest(data []byte) (RequestType, []byte, error) {
	if len(data) < 5 {
		return 0, nil, fmt.Errorf("request too short")
	}
	length := int(getUint32(data[:4]))
	if length != len(data)-4 {
		return 0, nil, fmt.Errorf("invalid request length")
	}
	reqType := RequestType(data[4])
	body := data[5:]
	return reqType, body, nil
}

func EncodeFetchRequest(req FetchRequest) ([]byte, error) {
	topicLen := len(req.Topic)
	totalLen := 4 + topicLen + 4 + 8
	result := make([]byte, totalLen)
	off := 0
	putUint32(result[off:], uint32(topicLen))
	off += 4
	copy(result[off:], req.Topic)
	off += topicLen
	putInt32(result[off:], req.PartitionId)
	off += 4
	putInt64(result[off:], req.Offset)
	return result, nil
}

func DecodeFetchRequest(data []byte) (FetchRequest, error) {
	if len(data) < 4 {
		return FetchRequest{}, fmt.Errorf("fetch request too short")
	}
	off := 0
	topicLen := int(getUint32(data[off:]))
	off += 4
	if len(data) < off+topicLen+4+8 {
		return FetchRequest{}, fmt.Errorf("fetch request too short")
	}
	topic := string(data[off : off+topicLen])
	off += topicLen
	partitionID := getInt32(data[off:])
	off += 4
	offset := getInt64(data[off:])
	return FetchRequest{
		Topic:       topic,
		PartitionId: partitionID,
		Offset:      offset,
	}, nil
}

func EncodeCreateTopicRequest(req CreateTopicRequest) ([]byte, error) {
	topicLen := len(req.Topic)
	totalLen := 4 + topicLen + 4
	result := make([]byte, totalLen)
	off := 0
	putUint32(result[off:], uint32(topicLen))
	off += 4
	copy(result[off:], req.Topic)
	off += topicLen
	putInt32(result[off:], req.PartitionCount)
	return result, nil
}

func DecodeCreateTopicRequest(data []byte) (CreateTopicRequest, error) {
	if len(data) < 4 {
		return CreateTopicRequest{}, fmt.Errorf("create topic request too short")
	}
	off := 0
	topicLen := int(getUint32(data[off:]))
	off += 4
	if len(data) < off+topicLen+4 {
		return CreateTopicRequest{}, fmt.Errorf("create topic request too short")
	}
	topic := string(data[off : off+topicLen])
	off += topicLen
	partitionCount := getInt32(data[off:])
	return CreateTopicRequest{
		Topic:          topic,
		PartitionCount: partitionCount,
	}, nil
}

func EncodeListTopicsRequest() ([]byte, error) {
	return []byte{0x01, 0x00, 0x00, 0x00, byte(ListTopicsRequestType)}, nil
}

func DecodeListTopicsRequest(data []byte) error {
	if len(data) != 5 {
		return fmt.Errorf("invalid list topics request length")
	}
	length := getUint32(data[:4])
	if length != 1 {
		return fmt.Errorf("invalid list topics request length: %d", length)
	}
	reqType := RequestType(data[4])
	if reqType != ListTopicsRequestType {
		return fmt.Errorf("expected ListTopicsRequest, got %d", reqType)
	}
	return nil
}
