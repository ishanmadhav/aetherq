package broker

import (
	"encoding/json"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/ishanmadhav/aetherq/api"
)

func (b *Broker) StartServer() {
	b.setupRoutes()
	b.server.Listen(b.brokerURI)
}

func (b *Broker) setupRoutes() {
	b.server.Get("/topics", b.getAllTopicsController)
	b.server.Post("/topic", b.createTopicController)
	b.server.Get("/consume", b.consumeController)
	b.server.Post("/produce", b.produceController)

}

func (b *Broker) getAllTopicsController(c *fiber.Ctx) error {
	return c.JSON(b.Topics)
}

type CreateTopicRequest struct {
	Name              string `json:"name"`
	ReplicationFactor int    `json:"replicationFactor"`
	PartitionCount    int    `json:"partitionCount"`
}

func (b *Broker) createTopicController(c *fiber.Ctx) error {
	var req CreateTopicRequest
	err := c.BodyParser(&req)
	if err != nil {
		return c.JSON(err)
	}
	err = b.CreateTopic(req.Name, req.ReplicationFactor, req.PartitionCount)
	if err != nil {
		return c.JSON(err)
	}
	b.AppendTopicLoop()
	var resp struct {
		Message string `json:"message"`
	}
	resp.Message = "Topic created successfully"
	return c.JSON(resp)
}

func (b *Broker) consumeController(c *fiber.Ctx) error {
	var req api.FetchMessage
	err := json.Unmarshal(c.Body(), &req)
	if err != nil {
		return c.JSON(err)
	}
	message, err := b.Consume(req)
	if err != nil {
		return c.JSON(err)
	}

	return c.JSON(message)
}

func (b *Broker) produceController(c *fiber.Ctx) error {
	var msg api.Message
	fmt.Println(string(c.Body()))
	err := c.BodyParser(&msg)
	if err != nil {
		fmt.Println("Error in parsing body")
		fmt.Println(err)
		return c.Status(400).JSON(err)
	}

	err = b.Produce(msg)
	if err != nil {
		return c.Status(400).JSON(err)
	}
	b.AppendEntriesLoop(msg)
	return c.JSON("produce")
}
