package broker

import (
	"github.com/ishanmadhav/aetherq/api"
	"github.com/ishanmadhav/aetherq/pkg/partition"
	"github.com/ishanmadhav/aetherq/pkg/topic"
)

//Topic Manager functions

// CreateTopic creates a new topic
func (b *Broker) CreateTopic(name string, replicationFactor int, partitionCount int) error {
	topic := topic.NewTopic(name, replicationFactor, partitionCount)
	b.Topics = append(b.Topics, &topic)
	return nil
}

// Consunmer Manger functions
func (b *Broker) Consume(fetchReq api.FetchMessage) (api.Message, error) {
	topicName, partitionNum := extractTopicAndPartition(fetchReq.TopicPartition)
	var topic *topic.Topic
	for i := 0; i < len(b.Topics); i++ {
		if b.Topics[i].Name == topicName {
			topic = b.Topics[i]
			break
		}
	}

	var p *partition.Partition
	for i := 0; i < len(topic.Partitions); i++ {
		if topic.Partitions[i].PartitionNumber == partitionNum {
			p = topic.Partitions[i]
			break
		}
	}
	msg, err := p.Read(int64(fetchReq.Offset))
	if err != nil {
		return api.Message{}, err
	}

	return msg, nil
}

// Producer Manager functions
func (b *Broker) Produce(msg api.Message) error {
	topicName, partitionNum := extractTopicAndPartition(msg.TopicPartition)
	var topic *topic.Topic
	for i := 0; i < len(b.Topics); i++ {
		if b.Topics[i].Name == topicName {
			topic = b.Topics[i]
			break
		}
	}

	var p *partition.Partition
	for i := 0; i < len(topic.Partitions); i++ {
		if topic.Partitions[i].PartitionNumber == partitionNum {
			p = topic.Partitions[i]
			break
		}
	}

	err := p.Write(msg)
	if err != nil {
		return err
	}
	return nil
}
