package test

import (
"fmt"
"github.com/confluentinc/confluent-kafka-go/kafka"
"log"
"os"
"os/signal"
"syscall"
)

// https://support.huaweicloud.com/devg-kafka/kafka-go.html

func TestConsumer() {
	log.Println("Starting a new kafka consumer")

	config := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          group,
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Panicf("Error creating consumer: %v", err)
		return
	}

	err = consumer.SubscribeTopics([]string{topics}, nil)
	if err != nil {
		log.Panicf("Error subscribe consumer: %v", err)
		return
	}

	go func() {
		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				log.Printf("Consumer error: %v (%v)", err, msg)
			} else {
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	if err = consumer.Close(); err != nil {
		log.Panicf("Error closing consumer: %v", err)
	}
}
