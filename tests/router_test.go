package tinykafka_test

import (
	"testing"

	"tinykafka-go/tinykafka"
)

func TestRouter_GetWorker(t *testing.T) {
	b := tinykafka.NewBroker(t.TempDir())
	b.CreateTopic("t", 1)
	w0 := tinykafka.NewWorker(b)
	w1 := tinykafka.NewWorker(b)

	r := tinykafka.NewRouter([]*tinykafka.Worker{w0, w1})

	worker, err := r.GetWorker(0)
	if err != nil {
		t.Fatal(err)
	}
	if worker != w0 {
		t.Fatal("partition 0 should route to worker 0")
	}

	worker, err = r.GetWorker(1)
	if err != nil {
		t.Fatal(err)
	}
	if worker != w1 {
		t.Fatal("partition 1 should route to worker 1")
	}
}

func TestRouter_SamePartitionGoesToSameWorker(t *testing.T) {
	b := tinykafka.NewBroker(t.TempDir())
	w0 := tinykafka.NewWorker(b)
	w1 := tinykafka.NewWorker(b)
	r := tinykafka.NewRouter([]*tinykafka.Worker{w0, w1})

	a, _ := r.GetWorker(4)
	c, _ := r.GetWorker(4)
	if a != c {
		t.Fatal("same partition should route to same worker")
	}
}

func TestRouter_DifferentPartitionsMayRouteDifferently(t *testing.T) {
	b := tinykafka.NewBroker(t.TempDir())
	w0 := tinykafka.NewWorker(b)
	w1 := tinykafka.NewWorker(b)
	r := tinykafka.NewRouter([]*tinykafka.Worker{w0, w1})

	worker0, _ := r.GetWorker(0)
	worker1, _ := r.GetWorker(1)
	if worker0 == nil || worker1 == nil {
		t.Fatal("workers should not be nil")
	}
}

func TestRouter_AllRoutesWithinBounds(t *testing.T) {
	b := tinykafka.NewBroker(t.TempDir())
	workers := make([]*tinykafka.Worker, 4)
	for i := range workers {
		workers[i] = tinykafka.NewWorker(b)
	}
	r := tinykafka.NewRouter(workers)

	for i := 0; i < 100; i++ {
		worker, err := r.GetWorker(i)
		if err != nil {
			t.Fatal(err)
		}
		if worker == nil {
			t.Fatalf("partition %d returned nil worker", i)
		}
	}
}

func TestRouter_NoWorkersReturnsError(t *testing.T) {
	r := tinykafka.NewRouter([]*tinykafka.Worker{})

	_, err := r.GetWorker(0)
	if err == nil {
		t.Fatal("expected error with no workers")
	}
}

func TestRouter_SingleWorkerAlwaysReturnsIt(t *testing.T) {
	b := tinykafka.NewBroker(t.TempDir())
	w := tinykafka.NewWorker(b)
	r := tinykafka.NewRouter([]*tinykafka.Worker{w})

	for i := 0; i < 10; i++ {
		worker, err := r.GetWorker(i)
		if err != nil {
			t.Fatal(err)
		}
		if worker != w {
			t.Fatalf("single worker router should always return the same worker")
		}
	}
}
