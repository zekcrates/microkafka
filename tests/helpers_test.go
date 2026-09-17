package tinykafka_test

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"testing"

	"tinykafka-go/tinykafka"
)

func tempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func setupLog(t *testing.T) *tinykafka.Log {
	t.Helper()
	l := &tinykafka.Log{Filename: filepath.Join(tempDir(t), "test.log")}
	if err := l.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func setupBroker(t *testing.T) *tinykafka.Broker {
	t.Helper()
	b := tinykafka.NewBroker(t.TempDir())
	t.Cleanup(func() { b.Close() })
	return b
}

func setupWorker(t *testing.T) *tinykafka.Worker {
	t.Helper()
	b := tinykafka.NewBroker(t.TempDir())
	w := tinykafka.NewWorker(b)
	t.Cleanup(func() { b.Close() })
	return w
}

func startTestServer(t testing.TB, topics map[string]int) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	broker := tinykafka.NewBroker(dir)
	for name, partitions := range topics {
		broker.CreateTopic(name, partitions)
	}
	if err := broker.Open(); err != nil {
		t.Fatal(err)
	}

	router := tinykafka.NewRouter([]*tinykafka.Worker{tinykafka.NewWorker(broker)})
	srv := tinykafka.NewServer(router, broker)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.HandleConn(conn)
		}
	}()

	cleanup := func() {
		ln.Close()
		broker.Close()
	}
	return ln.Addr().String(), cleanup
}

type bufferedConn struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
}

func dialBuffered(addr string) (*bufferedConn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &bufferedConn{
		conn: conn,
		br:   bufio.NewReaderSize(conn, 65536),
		bw:   bufio.NewWriterSize(conn, 65536),
	}, nil
}

func (bc *bufferedConn) close() {
	bc.conn.Close()
}

func readResponseFrom(br io.Reader) (byte, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	length := binary.LittleEndian.Uint32(lenBuf[:])
	data := make([]byte, length)
	if _, err := io.ReadFull(br, data); err != nil {
		return 0, nil, err
	}
	return data[0], data[1:], nil
}

func sendRequestTo(bw *bufio.Writer, conn net.Conn, reqType tinykafka.RequestType, body []byte) error {
	req, err := tinykafka.EncodeRequest(reqType, body)
	if err != nil {
		return err
	}
	if _, err := bw.Write(req); err != nil {
		return err
	}
	return bw.Flush()
}

func createTopicViaTCP(bc *bufferedConn, topic string, partitions int) error {
	body, err := tinykafka.EncodeCreateTopicRequest(tinykafka.CreateTopicRequest{
		Topic:          topic,
		PartitionCount: int32(partitions),
	})
	if err != nil {
		return err
	}
	if err := sendRequestTo(bc.bw, bc.conn, tinykafka.CreateTopicRequestType, body); err != nil {
		return err
	}
	_, _, err = readResponseFrom(bc.br)
	return err
}

func listTopicsViaTCP(bc *bufferedConn) ([]string, error) {
	body, err := tinykafka.EncodeListTopicsRequest()
	if err != nil {
		return nil, err
	}
	if err := sendRequestTo(bc.bw, bc.conn, tinykafka.ListTopicsRequestType, body); err != nil {
		return nil, err
	}
	_, respBody, err := readResponseFrom(bc.br)
	if err != nil {
		return nil, err
	}
	resp, err := tinykafka.DecodeListTopicsResponse(respBody)
	if err != nil {
		return nil, err
	}
	return resp.Topics, nil
}

func produceViaTCP(bc *bufferedConn, topic string, partition int, msg []byte) (int64, error) {
	body, err := tinykafka.EncodeProduceRequest(tinykafka.ProduceRequest{
		Topic:       topic,
		PartitionId: int32(partition),
		Message:     msg,
	})
	if err != nil {
		return 0, err
	}
	if err := sendRequestTo(bc.bw, bc.conn, tinykafka.ProduceRequestType, body); err != nil {
		return 0, err
	}
	_, respBody, err := readResponseFrom(bc.br)
	if err != nil {
		return 0, err
	}
	resp, err := tinykafka.DecodeProduceResponse(respBody)
	if err != nil {
		return 0, err
	}
	return resp.Offset, nil
}

func fetchViaTCP(bc *bufferedConn, topic string, partition int, offset int64) ([]byte, error) {
	body, err := tinykafka.EncodeFetchRequest(tinykafka.FetchRequest{
		Topic:       topic,
		PartitionId: int32(partition),
		Offset:      offset,
	})
	if err != nil {
		return nil, err
	}
	if err := sendRequestTo(bc.bw, bc.conn, tinykafka.FetchRequestType, body); err != nil {
		return nil, err
	}
	_, respBody, err := readResponseFrom(bc.br)
	if err != nil {
		return nil, err
	}
	resp, err := tinykafka.DecodeFetchResponse(respBody)
	if err != nil {
		return nil, err
	}
	return resp.Message, nil
}
