package main

import "github.com/ishanmadhav/aetherq/pkg/client"

func main() {
	cli, err := client.NewClient(client.ClientOpts{
		BrokerUri: ":3000",
		RPCUri:    "localhost:4001",
	})
	if err != nil {
		panic(err)
	}

	err = cli.Print()
	if err != nil {
		panic(err)
	}

}
