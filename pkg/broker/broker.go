package broker

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/ishanmadhav/aetherq/pkg/topic"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Broker struct {
	Name       string
	Peers      []Peer
	Topics     []*topic.Topic
	etcdClient *clientv3.Client
	server     *fiber.App
}

type Peer struct {
}

func NewBroker(name string) (*Broker, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	app := fiber.New()
	broker := &Broker{
		Name:       name,
		etcdClient: cli,
		Peers:      []Peer{},
		Topics:     []*topic.Topic{},
		server:     app,
	}
	return broker, nil
}

func (b *Broker) GetPeers() []Peer {
	return b.Peers
}
