package main

import (
	"fmt"
	"log"

	"tinykafka-go/tinykafka"
)

func main() {
	l := tinykafka.Log{Filename: "test.log"}

	if err := l.Open(); err != nil {
		log.Fatal(err)
	}

	offset1, err := l.AppendMessage([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}

	offset2, err := l.AppendMessage([]byte("world"))
	if err != nil {
		log.Fatal(err)
	}

	offset3, err := l.AppendMessage([]byte("tiny kafka"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("offsets:", offset1, offset2, offset3)

	msg1, err := l.ReadMessage(offset1)
	if err != nil {
		log.Fatal(err)
	}

	msg2, err := l.ReadMessage(offset2)
	if err != nil {
		log.Fatal(err)
	}

	msg3, err := l.ReadMessage(offset3)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(msg1))
	fmt.Println(string(msg2))
	fmt.Println(string(msg3))

	if err := l.Close(); err != nil {
		log.Fatal(err)
	}

	l = tinykafka.Log{Filename: "test.log"}

	if err := l.Open(); err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	fmt.Println("next offset after reopen:", l.NextOffset)
}
