package topic

import (
	"fmt"

	"github.com/ishanmadhav/aetherq/pkg/consumer"
	"github.com/ishanmadhav/aetherq/pkg/partition"
	"github.com/ishanmadhav/aetherq/pkg/producer"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Topic struct {
	Name              string
	ReplicationFactor int
	PartitionCount    int
	Partitions        []*partition.Partition
	Consumers         []consumer.Consumer
	Producers         []producer.Producer
}

func NewTopic(name string, replicationFactor int, PartitionCount int, brokerURI string, etcdClient *clientv3.Client) Topic {
	topic := Topic{
		Name:              name,
		ReplicationFactor: replicationFactor,
		PartitionCount:    PartitionCount,
	}
	for i := 0; i < PartitionCount; i++ {
		p, err := partition.NewPartition(topic.Name, i, brokerURI, etcdClient)
		if err != nil {
			fmt.Println("Could not create Partition")
			break
		}
		topic.Partitions = append(topic.Partitions, p)
	}
	return topic
}
