package tinykafka

import "fmt"

type Router struct {
	Workers []*Worker
}

func NewRouter(workers []*Worker) *Router {
	return &Router{
		Workers: workers,
	}
}

func (r *Router) GetWorker(partitionID int) (*Worker, error) {
	if len(r.Workers) == 0 {
		return nil, fmt.Errorf("no workers available")

	}

	workerID := partitionID % len(r.Workers)

	return r.Workers[workerID], nil
}
