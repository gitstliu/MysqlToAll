package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gitstliu/log4go"
)

type Client struct {
	Producer *kafka.Producer
	Config   *ClientConfig
}

type ClientConfig struct {
	Address string
}

func NewClient(conf *ClientConfig) *Client {
	client := &Client{Config: conf}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": conf.Address})
	if err != nil {
		log4go.Error(err)
		return nil
	}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log4go.Error("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log4go.Debug("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	client.Producer = p

	return client
}

func (this *Client) Close() {
	this.Producer.Close()
}

func (this *Client) SendMessages(messages []string, topics []string) error {

	var err error = nil
	for _, currMessage := range messages {
		for _, currTopic := range topics {
			produceErr := this.Producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &currTopic, Partition: kafka.PartitionAny},
				Value:          []byte(currMessage),
			}, nil)
			if produceErr != nil {
				log4go.Error(produceErr)
				err = produceErr
			}
		}
	}

	return err
}

func (this *Client) FlushAll() {
	for true {
		if this.Producer.Flush(100) == 0 {
			log4go.Debug("Flush message to kafka finished")
			break
		}
	}
}
