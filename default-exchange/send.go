package main

import (
	"github.com/streadway/amqp"
	"log"
)

//admin:admin123 (admin account)
//ops0:ops0 (msg producer account)
//ops1:ops1 (msg consumer account)
//python 3.9

func main() {
	// Create a new RabbitMQ connection.
	connection, err := amqp.Dial("amqp://ops0:ops0@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer connection.Close()
	// Let's start by opening a channel to our RabbitMQ
	// instance over the connection we have already
	// established.
	channel, err := connection.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()
	q, err := channel.QueueDeclare(
		"mytest", // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	body := "Hello World!"
	err = channel.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
