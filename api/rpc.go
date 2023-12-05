package api

type AppendEntriesArgs struct {
	Messages []Message `json:"messages"`
}

type AppendEntriesReply struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
}

type AppendTopicArgs struct {
	TopicName         string `json:"name"`
	ReplicationFactor int    `json:"replicationFactor"`
	PartitionCount    int    `json:"partitionCount"`
}

type AppendTopicReply struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
}
