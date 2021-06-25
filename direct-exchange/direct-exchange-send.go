package main

import (
	"log"

	"github.com/streadway/amqp"
)

func main() {
	// Create a new RabbitMQ connection.
	connection, err := amqp.Dial("amqp://ops0:ops0@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connection.Close()
	// Let's start by opening a ch to our RabbitMQ
	// instance over the connection we have already
	// established.
	ch, err := connection.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	err = ch.Confirm(false)
	err = ch.ExchangeDeclare("test-direct-exchange", "direct", true, false, false, false, nil)
	failOnError(err, "Failed to create exchange")

	body := "Hello World!"
	err = ch.Publish(
		"test-direct-exchange", // exchange
		"test-key",             // routing key
		false,                  // mandatory
		false,                  // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(body),
			DeliveryMode: 2,
		})
	failOnError(err, "Failed to publish a message")
	//err = ch.ExchangeDelete("test-direct-exchange", false, false)
	//failOnError(err, "Failed to delete exchange")
	log.Printf(" [x] Sent %s", body)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
