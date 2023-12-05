package broker

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/ishanmadhav/aetherq/api"
	clientv3 "go.etcd.io/etcd/client/v3"
)

//Non RPC functions

// Equivalent of GetPeers function
func (b *Broker) GetPeerList() ([]PeerData, error) {
	ctx := context.Background()
	resp, err := b.etcdClient.Get(ctx, "raft/peers")
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return []PeerData{}, nil
	}
	var peerDataArray []PeerData
	err = json.Unmarshal(resp.Kvs[0].Value, &peerDataArray)
	if err != nil {
		return nil, err
	}
	return peerDataArray, nil
}

// Checks if peer is already in maintained list
func (b *Broker) CheckIfPeerAlreadyInList(peer []PeerData) bool {
	peerList, err := b.GetPeerList()
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(peerList); i++ {
		if peerList[i].Id == b.Id {
			return true
		}
	}
	return false
}

// Adds peer to list
func (b *Broker) AddPeerToList(peerDataArray []PeerData) error {
	ctx := context.Background()
	peerDataArrayBytes, err := json.Marshal(peerDataArray)
	if err != nil {
		return err
	}
	_, err = b.etcdClient.Put(ctx, "raft/peers", string(peerDataArrayBytes))
	if err != nil {
		return err
	}
	return nil
}

// Clears the cluster from etcd
func (b *Broker) ClearCluster() error {
	ctx := context.Background()

	_, err := b.etcdClient.Delete(ctx, "raft/peers", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	return nil
}

// Keeps checking if new peers have been added in the kv store
func (b *Broker) PeerRefreshLoop() {

}

// Starts the server for raft related RPC calls
func (b *Broker) StartRPCServer(wg *sync.WaitGroup) {
	defer wg.Done()
	err := rpc.Register(b)
	if err != nil {
		panic(err)
	}
	listener, err := net.Listen("tcp", b.rpcURI)
	log.Println("Listening on ", b.rpcURI)
	if err != nil {
		panic(err)
	}
	rpc.Accept(listener)
}

func (b *Broker) AppendEntriesLoop(msg api.Message) {
	var failed bool
	failed = false
	for i := 0; i < len(b.Peers); i++ {
		var args api.AppendEntriesArgs
		args.Messages = append(args.Messages, msg)
		var reply api.AppendEntriesReply
		err := b.Peers[i].rpcClient.Call("Broker.AppendEntriesRPC", &args, &reply)
		if err != nil {
			log.Println(err)
		}
		if !reply.Success {
			log.Println("Error in appending entries")
			log.Println(reply.Message)
			failed = true
		}
	}
	if failed {
		log.Println("Failed to append entries")
	} else {
		log.Println("Successfully appended entries")
	}

}

func (b *Broker) AppendTopicLoop() {
	var failed bool = false
	for i := 0; i < len(b.Peers); i++ {
		var args api.AppendTopicArgs
		args.TopicName = b.Topics[len(b.Topics)-1].Name
		args.ReplicationFactor = b.Topics[len(b.Topics)-1].ReplicationFactor
		args.PartitionCount = b.Topics[len(b.Topics)-1].PartitionCount
		var reply api.AppendTopicReply
		err := b.Peers[i].rpcClient.Call("Broker.AppendTopicRPC", &args, &reply)
		if err != nil {
			log.Println(err)
		}
		if !reply.Success {
			log.Println("Error in appending topic")
			log.Println(reply.Message)
			failed = true
		}
	}
	if failed {
		log.Println("Failed to append topic")
	} else {
		log.Println("Successfully appended topic")
	}
}

//RPC functions

func (b *Broker) PrintRPC(args *int, reply *int) error {
	log.Println("RPC called")
	return nil
}

func (b *Broker) AppendEntriesRPC(args *api.AppendEntriesArgs, reply *api.AppendEntriesReply) error {
	log.Println("AppendRPC called")

	for i := 0; i < len(args.Messages); i++ {
		log.Println(args.Messages[i])
		err := b.Produce(args.Messages[i])
		if err != nil {
			reply.Success = false
			reply.Message = err.Error()
			continue
		}
	}
	return nil
}

func (b *Broker) AppendTopicRPC(args *api.AppendTopicArgs, reply *api.AppendTopicReply) error {
	log.Println("AppendTopicRPC called")
	err := b.CreateTopic(args.TopicName, args.ReplicationFactor, args.PartitionCount)
	if err != nil {
		reply.Message = err.Error()
		reply.Success = false
		return nil
	}
	reply.Message = "Topic created successfully"
	reply.Success = true
	return nil
}
