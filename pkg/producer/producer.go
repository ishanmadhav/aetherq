package producer

import (
	"encoding/json"
	"fmt"

	"github.com/ishanmadhav/aetherq/api"
	"github.com/valyala/fasthttp"
)

const BROKER_URL = "http://localhost:3000"

// Producer for queue events/messages
type Producer struct {
	httpClient *fasthttp.Client
}

type ProducerConfig struct {
}

func NewProducer(config ProducerConfig) *Producer {
	var httpClient *fasthttp.Client = &fasthttp.Client{}
	return &Producer{
		httpClient: httpClient,
	}
}

func (p *Producer) Produce(msg api.Message) error {
	fmt.Println("This is running")
	url := BROKER_URL + "/produce"
	req := fasthttp.AcquireRequest()
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")
	json, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	req.SetBody(json)

	resp := fasthttp.AcquireResponse()
	err = p.httpClient.Do(req, resp)
	if err != nil {
		return err
	}

	if resp.StatusCode() != 200 {
		fmt.Println("Error")
		fmt.Println(string(resp.Body()))
		return err
	}

	fmt.Println(string(resp.Body()))

	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	return nil
}

func (p *Producer) ProduceBatch() error {
	return nil
}
