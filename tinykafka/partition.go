package tinykafka

import "sync"

type Partition struct {
	Log *Log
	mu  sync.Mutex
}

func (p *Partition) Open() error {
	return p.Log.Open()
}

func (p *Partition) AppendMessage(message []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Log.AppendMessage(message)
}

func (p *Partition) ReadMessage(offset int64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Log.ReadMessage(offset)
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Log.Close()
}
