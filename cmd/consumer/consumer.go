package main

import (
	"fmt"
	"time"

	"github.com/ishanmadhav/aetherq/pkg/consumer"
)

func main() {
	config := consumer.ConsumerConfig{}
	c := consumer.NewConsumer(config)
	c.SubscribeToTopic("demoTopic", 1)

	run := true

	for run {
		ev, err := c.ReadMessage(1000 * time.Millisecond)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(ev.Value))
	}
}
