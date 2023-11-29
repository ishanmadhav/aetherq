package api

import "time"

type Message struct {
	TopicPartition TopicPartition `json:"topic_partition"`
	Value          []byte         `json:"value"`
	Key            []byte         `json:"key"`
	Timestap       time.Time      `json:"timestamp"`
}

type MessageRequest struct {
	TopicPartition TopicPartition `json:"topic_partition"`
	Value          string         `json:"value"`
	Key            string         `json:"key"`
}

type FetchMessage struct {
	Offset         int            `json:"offset"`
	TopicPartition TopicPartition `json:"topic_Partition"`
}

func (m *Message) String() string {
	return string(m.Value)
}
