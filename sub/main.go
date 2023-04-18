package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

/**

producer ->   消息提供者 -> 推送消息得工具.

consumer ->    消费者   -> 消费消息得地方

consumer-group  -> 绑定多个 topic. 消费者绑定一个topic



*/

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0

	client, err := sarama.NewConsumerGroup([]string{"192.168.1.101:9092"}, `wechat_msg_group_p`, config)
	if err != nil {
		panic(err)
	}

	if err := client.Consume(context.Background(), []string{"wechat_msg_p"}, &handler{}); err != nil {
		panic(err)
	}
}

type handler struct {
}

func (h *handler) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Println("setup")
	return nil
}

func (h *handler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("Cleanup")
	return nil
}

func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		time.Sleep(5 * time.Second)
		fmt.Println(string(msg.Value))
	}

	return nil
}
