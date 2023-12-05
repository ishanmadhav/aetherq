package client

import (
	"fmt"
	"net/rpc"
)

type Client struct {
	rpcCli *rpc.Client
}

type ClientOpts struct {
	BrokerUri string `json:"broker_uri"`
	RPCUri    string `json:"rpc_uri"`
}

func NewClient(opts ClientOpts) (*Client, error) {
	cli, err := rpc.Dial("tcp", opts.RPCUri)
	if err != nil {
		return nil, err
	}
	return &Client{
		rpcCli: cli,
	}, nil
}

func (c *Client) Print() error {
	var reply int
	var args int
	err := c.rpcCli.Call("Broker.PrintRPC", &args, &reply)
	if err != nil {
		return err
	}
	fmt.Println("RPC was successful")
	return nil
}
