package tinykafka

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Broker struct {
	Topics  map[string]*Topic
	DataDir string
}

func (b *Broker) CreateTopic(name string, partitionCount int) error {
	topic := &Topic{
		Name: name,
	}

	topic.CreatePartitions(partitionCount)
	b.Topics[name] = topic
	return nil
}

func (b *Broker) GetTopic(name string) (*Topic, error) {

	topic, ok := b.Topics[name]

	if !ok {
		return nil, fmt.Errorf("topic %q not found", name)

	}

	return topic, nil
}

func (b *Broker) Open() error {
	for _, topic := range b.Topics {
		if err := topic.Open(); err != nil {
			return err
		}
	}

	return nil
}
func (b *Broker) HasTopic(name string) bool {
	_, ok := b.Topics[name]
	return ok
}
func (b *Broker) ListTopics() []string {
	topics := make([]string, 0, len(b.Topics))

	for name := range b.Topics {
		topics = append(topics, name)
	}

	return topics
}
func (b *Broker) Close() error {
	for _, topic := range b.Topics {
		if err := topic.Close(); err != nil {
			return err
		}
	}

	return nil
}
func NewBroker(dataDir string) *Broker {
	return &Broker{
		Topics:  make(map[string]*Topic),
		DataDir: dataDir,
	}
}

type TopicMetadata struct {
	Name           string `json:"name"`
	PartitionCount int    `json:"partition_count"`
}

func (b *Broker) SaveTopicsToFile() error {
	topics := make([]TopicMetadata, 0, len(b.Topics))
	for _, topic := range b.Topics {
		topics = append(topics, TopicMetadata{
			Name:           topic.Name,
			PartitionCount: len(topic.Partitions),
		})

	}
	data, err := json.MarshalIndent(topics, "", "  ")
	if err != nil {
		return err
	}

	filename := filepath.Join(b.DataDir, "topics.json")

	return os.WriteFile(filename, data, 0644)
}

func (b *Broker) LoadTopics() error {

	filename := filepath.Join(b.DataDir, "topics.json")
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err

	}
	var topics []TopicMetadata
	if err := json.Unmarshal(data, &topics); err != nil {
		return fmt.Errorf("decode topics: %w", err)
	}

	for _, metadata := range topics {
		topic := &Topic{
			Name: metadata.Name,
		}

		topic.CreatePartitions(metadata.PartitionCount)

		b.Topics[metadata.Name] = topic
	}

	return nil
}
