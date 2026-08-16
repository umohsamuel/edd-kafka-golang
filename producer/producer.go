package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

type Order struct {
	Text string `form:"text" json:"text"`
}

func main() {

	brokersUrl := []string{"localhost:9092"}

	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer producer.Close()

	router := gin.Default()

	api := router.Group("/api/v1")

	api.POST("/order", createOrderHandler(producer))

	router.Run()
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	sarama.MaxRequestSize = 100 * 1024 * 1024
	sarama.MaxResponseSize = 100 * 1024 * 1024
	config.Producer.MaxMessageBytes = 100 * 1024 * 1024

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Producer.Retry.Max = 3
	config.Net.MaxOpenRequests = 1

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil

}

func PushOrderToQueue(producer sarama.SyncProducer, topic string, message []byte) error {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)", topic, partition, offset)
	return nil
}

func createOrderHandler(producer sarama.SyncProducer) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := "orders"
		order := new(Order)

		if err := c.ShouldBindJSON(&order); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		orderInBytes, err := json.Marshal(order)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		err = PushOrderToQueue(producer, topic, orderInBytes)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "Order pushed successfully",
			"order":   order,
		})

	}
}
