package broker

import (
	"net/rpc"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/ishanmadhav/aetherq/pkg/topic"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
)

type Broker struct {
	Name        string
	Peers       []Peer
	Topics      []*topic.Topic
	etcdClient  *clientv3.Client
	server      *fiber.App
	brokerURI   string
	rpcURI      string
	Id          int
	Role        string
	CurrentTerm int
	VotedFor    int
}

type Peer struct {
	Data      PeerData
	rpcClient *rpc.Client
}

type PeerData struct {
	Id  int    `json:"id"`
	Uri string `json:"uri"`
}

type BrokerOpts struct {
	Id        int    `json:"id"`
	Name      string `json:"name"`
	BrokerUri string `json:"broker_uri"`
	RpcUri    string `json:"rpc_uri"`
	Role      string `json:"role"`
}

func NewBroker(opts BrokerOpts) (*Broker, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	app := fiber.New()
	broker := &Broker{
		Name:       opts.Name,
		etcdClient: cli,
		Peers:      []Peer{},
		Topics:     []*topic.Topic{},
		server:     app,
		brokerURI:  opts.BrokerUri,
		rpcURI:     opts.RpcUri,
		Id:         opts.Id,
	}
	return broker, nil
}

func (b *Broker) Start() {
	b.InitPeers()
	var wg sync.WaitGroup
	wg.Add(1)
	go b.StartRPCServer(&wg)
	go b.StartServer()
	wg.Wait()
}

func (b *Broker) InitPeers() {
	peerDataArray, err := b.GetPeerList()
	if err != nil {
		panic(err)
	}
	if len(peerDataArray) == 0 {
		err = b.AddPeerToList(peerDataArray)
		if err != nil {
			panic(err)
		}
	}

	if !b.CheckIfPeerAlreadyInList(peerDataArray) {
		peerDataArray = append(peerDataArray, PeerData{
			Id:  b.Id,
			Uri: b.rpcURI,
		})
		err = b.AddPeerToList(peerDataArray)
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < len(peerDataArray); i++ {
		if peerDataArray[i].Id != b.Id {
			rpcClient, err := rpc.Dial("tcp", peerDataArray[i].Uri)
			if err != nil {
				continue
			}
			b.Peers = append(b.Peers, Peer{Data: peerDataArray[i], rpcClient: rpcClient})
		}
	}

}

func (b *Broker) GetPeers() []Peer {
	return b.Peers
}
