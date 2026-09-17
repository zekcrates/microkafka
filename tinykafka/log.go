package tinykafka

import (
	"encoding/binary"
	"io"
	"os"
)

type Log struct {
	NextOffset int64
	Filename   string
	file       *os.File
}

func (l *Log) Open() error {
	file, err := os.OpenFile(
		l.Filename,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644,
	)
	if err != nil {
		return err
	}
	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	l.file = file
	l.NextOffset = offset
	return nil
}

func (l *Log) AppendMessage(message []byte) (int64, error) {
	offset := l.NextOffset
	file := l.file

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(message)))

	if _, err := file.Write(lenBuf[:]); err != nil {
		return 0, err
	}
	if _, err := file.Write(message); err != nil {
		return 0, err
	}
	l.NextOffset += int64(4 + len(message))
	return offset, nil
}

func (l *Log) ReadMessage(offset int64) ([]byte, error) {
	var lenBuf [4]byte

	_, err := l.file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	_, err = io.ReadFull(l.file, lenBuf[:])
	if err != nil {
		return nil, err
	}
	msgLen := binary.LittleEndian.Uint32(lenBuf[:])

	msg := make([]byte, msgLen)

	_, err = io.ReadFull(l.file, msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (l *Log) Close() error {
	if l.file == nil {
		return nil
	}

	err := l.file.Close()

	l.file = nil

	return err
}
