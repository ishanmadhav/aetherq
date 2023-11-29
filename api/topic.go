package api

type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Metadata  string `json:"metadata"`
}
