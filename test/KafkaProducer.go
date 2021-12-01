package test

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka" // indirect
)

// https://support.huaweicloud.com/devg-kafka/kafka-go.html
func TestProducer() {
	log.Println("Starting a new kafka producer")

	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
	}
	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Panicf("producer error, err: %v", err)
		return
	}

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	fmt.Println("please enter message:")
	go func() {
		for {
			err := producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topics, Partition: kafka.PartitionAny},
				Value:          getInput(),
			}, nil)
			if err != nil {
				log.Panicf("send message fail, err: %v", err)
				return
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	// Wait for message deliveries before shutting down
	producer.Flush(15 * 1000)
	producer.Close()
}

func getInput() []byte {
	reader := bufio.NewReader(os.Stdin)
	data, _, _ := reader.ReadLine()
	return data
}
