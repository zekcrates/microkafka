package tinykafka

import "fmt"

type ProduceResponse struct {
	Offset int64
}

type FetchResponse struct {
	Message []byte
}

type CreateTopicResponse struct{}

type ListTopicsResponse struct {
	Topics []string
}

func EncodeProduceResponse(resp ProduceResponse) ([]byte, error) {
	result := make([]byte, 8)
	putInt64(result, resp.Offset)
	return result, nil
}

func DecodeProduceResponse(data []byte) (ProduceResponse, error) {
	var resp ProduceResponse
	if len(data) < 8 {
		return resp, fmt.Errorf("produce response too short")
	}
	resp.Offset = getInt64(data)
	return resp, nil
}

func EncodeFetchResponse(resp FetchResponse) ([]byte, error) {
	totalLen := 4 + len(resp.Message)
	result := make([]byte, totalLen)
	putUint32(result[:4], uint32(len(resp.Message)))
	copy(result[4:], resp.Message)
	return result, nil
}

func DecodeFetchResponse(data []byte) (FetchResponse, error) {
	var resp FetchResponse
	if len(data) < 4 {
		return resp, fmt.Errorf("fetch response too short")
	}
	msgLen := getUint32(data[:4])
	resp.Message = data[4 : 4+msgLen]
	return resp, nil
}

func EncodeCreateTopicResponse() ([]byte, error) {
	return []byte{}, nil
}

func EncodeListTopicsResponse(resp ListTopicsResponse) ([]byte, error) {
	totalLen := 4
	for _, name := range resp.Topics {
		totalLen += 4 + len(name)
	}
	result := make([]byte, totalLen)
	off := 0
	putUint32(result[off:], uint32(len(resp.Topics)))
	off += 4
	for _, name := range resp.Topics {
		putUint32(result[off:], uint32(len(name)))
		off += 4
		copy(result[off:], name)
		off += len(name)
	}
	return result, nil
}

func DecodeListTopicsResponse(data []byte) (ListTopicsResponse, error) {
	var resp ListTopicsResponse
	if len(data) < 4 {
		return resp, fmt.Errorf("list topics response too short")
	}
	count := getUint32(data[:4])
	off := 4
	for i := uint32(0); i < count; i++ {
		if len(data) < off+4 {
			return resp, fmt.Errorf("list topics response too short")
		}
		nameLen := int(getUint32(data[off:]))
		off += 4
		if len(data) < off+nameLen {
			return resp, fmt.Errorf("list topics response too short")
		}
		resp.Topics = append(resp.Topics, string(data[off:off+nameLen]))
		off += nameLen
	}
	return resp, nil
}
