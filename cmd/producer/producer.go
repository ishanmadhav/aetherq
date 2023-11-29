package main

import (
	"time"

	"github.com/ishanmadhav/aetherq/api"
	"github.com/ishanmadhav/aetherq/pkg/producer"
)

func main() {
	config := producer.ProducerConfig{}
	p := producer.NewProducer(config)
	topic := api.TopicPartition{
		Topic:     "demoTopic",
		Partition: 1,
		Metadata:  "test",
	}
	msg := api.Message{
		TopicPartition: topic,
		Key:            []byte("test"),
		Value:          []byte("Some message"),
		Timestap:       time.Now(),
	}
	err := p.Produce(msg)
	if err != nil {
		panic(err)
	}

}
