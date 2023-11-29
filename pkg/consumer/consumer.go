package consumer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ishanmadhav/aetherq/api"
	"github.com/valyala/fasthttp"
)

const BROKER_URL = "http://localhost:3000"

// Consumers will contains their own offset
// that will tell them till wher they have already fetched the messages
type Consumer struct {
	Offset     int
	Topic      string
	Partition  int
	httpClient *fasthttp.Client
}

type ConsumerConfig struct {
}

// NewConsumer will create a new consumer
func NewConsumer(conig ConsumerConfig) *Consumer {
	var httpClient *fasthttp.Client = &fasthttp.Client{}
	return &Consumer{
		Offset:     0,
		httpClient: httpClient,
	}
}

func (c *Consumer) SubscribeToTopic(topic string, Partition int) {
	c.Topic = topic
	c.Partition = Partition
}

func (c *Consumer) ReadMessage(interval time.Duration) (api.Message, error) {
	topic := api.TopicPartition{
		Topic:     c.Topic,
		Partition: c.Partition,
		Metadata:  "none",
	}
	fetchMessage := api.FetchMessage{
		Offset:         c.Offset,
		TopicPartition: topic,
	}
	url := BROKER_URL + "/consume"
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(url)
	req.Header.SetMethod("GET")
	req.Header.SetContentType("application/json")

	jsonstr, err := json.Marshal(fetchMessage)
	if err != nil {
		return api.Message{}, err
	}

	req.SetBody(jsonstr)
	resp := fasthttp.AcquireResponse()
	err = c.httpClient.Do(req, resp)
	if err != nil {
		return api.Message{}, err
	}

	if resp.StatusCode() != 200 {
		fmt.Println("Error")
		fmt.Println(string(resp.Body()))
		return api.Message{}, err
	}

	var message api.Message

	if err != nil {
		return api.Message{}, err
	}

	err = json.Unmarshal(resp.Body(), &message)
	if err != nil {
		return api.Message{}, err
	}

	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	//Increment the offset if new message received
	if message.Value != nil {
		c.Offset++
	}
	time.Sleep(interval)
	return message, nil

}
