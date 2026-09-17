package tinykafka

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

type Server struct {
	Router   *Router
	Broker   *Broker
	Worker   *Worker
	Metrics  *Metrics
	Logger   *Logger
	lock     sync.Mutex
	addr     string
	listener net.Listener
}

func NewServer(router *Router, broker *Broker) *Server {
	return &Server{
		Router:  router,
		Broker:  broker,
		Worker:  NewWorker(broker),
		Metrics: DefaultMetrics(),
		Logger:  defaultLogger,
	}
}

func (s *Server) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.lock.Lock()
	s.listener = listener
	s.addr = addr
	s.lock.Unlock()

	s.Logger.Info("server listening on %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go s.HandleConn(conn)
	}
}

func (s *Server) Addr() string {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Server) Stop() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) HandleConn(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReaderSize(conn, 65536)
	bw := bufio.NewWriterSize(conn, 65536)
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			return
		}
		length := binary.LittleEndian.Uint32(lenBuf[:])

		data := make([]byte, length)
		if _, err := io.ReadFull(br, data); err != nil {
			return
		}

		s.Metrics.RecordBytesRead(int64(4 + length))

		if len(data) < 1 {
			return
		}
		requestType := RequestType(data[0])
		body := data[1:]

		s.Metrics.RecordRequest(requestType)

		var respBody []byte
		var err error
		switch requestType {
		case CreateTopicRequestType:
			respBody, err = s.handleCreateTopic(body)
		case ListTopicsRequestType:
			respBody, err = s.handleListTopics(body)
		case ProduceRequestType:
			respBody, err = s.handleProduce(body)
		case FetchRequestType:
			respBody, err = s.handleFetch(body)
		default:
			err = fmt.Errorf("unknown request type: %d", requestType)
		}

		if err != nil {
			s.Metrics.RecordError()
			s.Logger.Error("handle request: %v", err)
			return
		}

		totalLen := uint32(1 + len(respBody))
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], totalLen)
		written := 4 + int(totalLen)

		if _, err := bw.Write(hdr[:]); err != nil {
			return
		}
		if _, err := bw.Write([]byte{byte(requestType)}); err != nil {
			return
		}
		if _, err := bw.Write(respBody); err != nil {
			return
		}
		if err := bw.Flush(); err != nil {
			return
		}

		s.Metrics.RecordBytesWritten(int64(written))
	}
}

func (s *Server) handleCreateTopic(body []byte) ([]byte, error) {
	req, err := DecodeCreateTopicRequest(body)
	if err != nil {
		return nil, err
	}
	s.Logger.Info("create topic %q with %d partitions", req.Topic, req.PartitionCount)
	if err := s.Broker.CreateTopic(req.Topic, int(req.PartitionCount)); err != nil {
		return nil, err
	}
	topic, err := s.Broker.GetTopic(req.Topic)
	if err != nil {
		return nil, err
	}
	if err := topic.Open(); err != nil {
		return nil, err
	}
	return EncodeCreateTopicResponse()
}

func (s *Server) handleListTopics(_ []byte) ([]byte, error) {
	topics := s.Broker.ListTopics()
	return EncodeListTopicsResponse(ListTopicsResponse{Topics: topics})
}

func (s *Server) handleProduce(body []byte) ([]byte, error) {
	req, err := DecodeProduceRequest(body)
	if err != nil {
		return nil, err
	}
	offset, err := s.Worker.AppendMessage(req.Topic, int(req.PartitionId), req.Message)
	if err != nil {
		return nil, err
	}
	s.Logger.Debug("produce topic=%s partition=%d offset=%d size=%d", req.Topic, req.PartitionId, offset, len(req.Message))
	return EncodeProduceResponse(ProduceResponse{Offset: offset})
}

func (s *Server) handleFetch(body []byte) ([]byte, error) {
	req, err := DecodeFetchRequest(body)
	if err != nil {
		return nil, err
	}
	msg, err := s.Worker.ReadMessage(req.Topic, int(req.PartitionId), req.Offset)
	if err != nil {
		return nil, err
	}
	return EncodeFetchResponse(FetchResponse{Message: msg})
}
