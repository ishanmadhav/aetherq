package partition

import (
	"bytes"
	"encoding/gob"

	"github.com/ishanmadhav/aetherq/api"
	"github.com/ishanmadhav/aetherq/pkg/storage"
)

// Partition is a partition of a topic.
// Each topic will consistof several partitions
// When a producer produces to a topic, we will pick the Partition in a load balanced manner
// Each consumer will only consume from one Partition
// Data storage will be done on a Partition level using log package
// all the on disk process will be done via the commit log
// We will always make sure the number of Partitions is more than the number of consumers
// one consumer can consume from two Partitions but no two consumers can consume from the same Partition
type Partition struct {
	TopicName       string
	PartitionNumber int
	CommitLog       *storage.CommitLog
}

func NewPartition(topicName string, partitionNumber int) (*Partition, error) {
	commitLog := storage.NewCommitLog()
	return &Partition{
		CommitLog:       commitLog,
		TopicName:       topicName,
		PartitionNumber: partitionNumber,
	}, nil
}

func (p *Partition) Write(msg api.Message) error {
	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	err := enc.Encode(msg)
	if err != nil {
		return err
	}

	p.CommitLog.Append(buf.Bytes())
	return nil
}

func (p *Partition) Read(offset int64) (api.Message, error) {
	res := p.CommitLog.Read(offset)
	var msg api.Message
	decoder := gob.NewDecoder(bytes.NewReader(res))
	err := decoder.Decode(&msg)
	if err != nil {
		return api.Message{}, err
	}
	return msg, nil
}
