package broker

import "github.com/ishanmadhav/aetherq/api"

func extractTopicAndPartition(topicPartition api.TopicPartition) (string, int) {
	return topicPartition.Topic, topicPartition.Partition
}
