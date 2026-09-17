package tinykafka

type Worker struct {
	Broker *Broker
}

func (w *Worker) AppendMessage(
	topicName string,
	partitionID int,
	message []byte,
) (int64, error) {
	topic, err := w.Broker.GetTopic(topicName)
	if err != nil {
		return 0, err
	}
	return topic.AppendMessage(partitionID, message)
}

func NewWorker(broker *Broker) *Worker {
	return &Worker{
		Broker: broker,
	}
}

func (w *Worker) ReadMessage(
	topicName string,
	partitionID int,
	offset int64,
) ([]byte, error) {
	topic, err := w.Broker.GetTopic(topicName)
	if err != nil {
		return nil, err

	}

	return topic.ReadMessage(partitionID, offset)
}
