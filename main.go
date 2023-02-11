package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

type category byte

const (
	Undefined category = iota
	Laptop
	Macbook
)

func (c category) String() string {
	return [...]string{"", "Laptop", "Macbook"}[c]
}

type Order struct {
	Category     category
	Price        float32
	Model        string
	Manufacturer string
}

type OrderLists []Order

func main() {
	var (
		topic = "orders"
	)

	go func() {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "foo",
			"auto.offset.reset": "smallest",
		})
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to create consumer: %s\n", err))
		}

		err = consumer.Subscribe(topic, nil)

		for {
			ev := consumer.Poll(100)

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("consumed orders %v\n", string(e.Value))
			case kafka.Error:
				log.Fatal(e)
			}
		}
	}()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "foo",
		"acks":              "all",
	})
	if err != nil {
		log.Fatal(fmt.Sprintf("Failed to create producer: %s\n", err))
	}

	var orders = OrderLists{
		Order{
			Category:     Laptop,
			Price:        25000000,
			Manufacturer: "Lenovo",
			Model:        "Thinkpad T14 Gen 3",
		},
		Order{
			Category:     Macbook,
			Price:        60000000,
			Manufacturer: "Apple",
			Model:        "Macbook Pro 13 M2 2022(32-512)",
		},
	}

	var ordersByte, _ = json.Marshal(orders)

	deliveryChan := make(chan kafka.Event, 10000)
	for {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          ordersByte,
		},
			deliveryChan,
		)
		//<-deliveryChan
		time.Sleep(time.Second * 5)
	}
}
