package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	api := router.Group("/api/v1")

	api.POST("/comments", createComment)
	router.Run()
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil

}

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

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

func createComment(c *gin.Context) {
	cmt := new(Comment)

	if err := c.ShouldBindJSON(&cmt); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err = PushCommentToQueue("comments", cmtInBytes)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Comment pushed successfully",
		"cmt":     cmt,
	})

}
