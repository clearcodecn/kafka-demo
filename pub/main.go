package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"time"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0
	config.Producer.Idempotent = true
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Net.MaxOpenRequests = 1

	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	producer, err := sarama.NewSyncProducer([]string{"192.168.1.101:9092"}, config)
	if err != nil {
		panic(err)
	}

	i := 0
	for {
		i++
		msg := WechatMessage{
			Content:    fmt.Sprintf("msg %d", i),
			From:       fmt.Sprintf("from %d", i),
			CreateTime: time.Now().String(),
		}
		data, _ := json.Marshal(msg)
		p, o, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic:     "wechat_msg_p",
			Value:     sarama.ByteEncoder(data),
			Partition: int32(i) % 8,
		})
		fmt.Println(p, o, err)
		time.Sleep(30 * time.Millisecond)
	}
}

type WechatMessage struct {
	Content    string `json:"content"`
	From       string `json:"from"`
	CreateTime string `json:"create_time"`
}
