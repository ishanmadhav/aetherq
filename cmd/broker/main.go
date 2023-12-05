package main

import "github.com/ishanmadhav/aetherq/pkg/broker"

func main() {
	opts := broker.BrokerOpts{
		Id:        1,
		Name:      "test",
		BrokerUri: ":3000",
		RpcUri:    "localhost:4001",
		Role:      "leader",
	}
	b, _ := broker.NewBroker(opts)
	//broker.CreateTopic("test", 1, 1)
	b.Start()
}
