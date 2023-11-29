package main

import "github.com/ishanmadhav/aetherq/pkg/broker"

func main() {
	b, _ := broker.NewBroker("test")
	//broker.CreateTopic("test", 1, 1)
	b.StartServer()
}
