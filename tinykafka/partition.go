package tinykafka

type Partition struct {
	Log *Log
}

func (p *Partition) Open() error {
	return p.Log.Open()
}

func (p *Partition) AppendMessage(message []byte) (int64, error) {
	return p.Log.AppendMessage(message)
}

func (p *Partition) ReadMessage(offset int64) ([]byte, error) {
	return p.Log.ReadMessage(offset)
}

func (p *Partition) Close() error {
	return p.Log.Close()
}
