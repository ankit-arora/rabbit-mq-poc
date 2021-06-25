package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://ops1:ops1@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	serverChannel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer serverChannel.Close()
	queueName := "rpc-server-queue"
	q, err := serverChannel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")
	err = serverChannel.Qos(1, 0, false)
	failOnError(err, "Failed to set qos")
	msgs, err := serverChannel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			replyQueueName := d.ReplyTo
			correlationID := d.CorrelationId
			log.Printf("Received a message: %s : %s", correlationID, d.Body)
			time.Sleep(10 * time.Second)
			response := "Response message: " + string(d.Body)
			err = serverChannel.Publish(
				"",             // exchange
				replyQueueName, // routing key
				false,          // mandatory
				false,          // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					Body:          []byte(response),
					CorrelationId: correlationID,
				})
			failOnError(err, "Failed to publish a message")
			serverChannel.Ack(d.DeliveryTag, false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
