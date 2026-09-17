package tinykafka

import "fmt"

type Topic struct {
	Name       string
	Partitions []*Partition
}

func (t *Topic) Open() error {
	for _, partition := range t.Partitions {
		if err := partition.Open(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Topic) Close() error {
	for _, partition := range t.Partitions {
		if err := partition.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Topic) AppendMessage(partitionId int, message []byte) (int64, error) {
	partition := t.Partitions[partitionId]
	return partition.AppendMessage(message)
}

func (t *Topic) ReadMessage(partitionId int, offset int64) ([]byte, error) {
	partition := t.Partitions[partitionId]
	return partition.ReadMessage(offset)

}

func (t *Topic) CreatePartitions(count int) {
	for i := 0; i < count; i++ {
		partition := &Partition{
			Log: &Log{
				Filename: fmt.Sprintf("%s-%d.log", t.Name, i),
			},
		}

		t.Partitions = append(t.Partitions, partition)
	}
}
