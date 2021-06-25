package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://ops1:ops1@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	err = ch.ExchangeDeclare("test-direct-exchange", "direct", true, false, false, false, nil)
	failOnError(err, "Failed to create exchange")
	//q, err := ch.QueueDeclare(
	//	"",    // name
	//	true,  // durable
	//	false, // delete when unused
	//	true,  // exclusive
	//	false, // no-wait
	//	nil,   // arguments
	//)
	q, err := ch.QueueDeclare(
		"mytest", // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")
	fmt.Println("queue name: " + q.Name)
	err = ch.QueueBind(q.Name, "test-key", "test-direct-exchange", false, nil)
	failOnError(err, "Failed to bind queue and exchange")
	ch.Qos(1, 0, false)
	msgs, err := ch.Consume(
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
			log.Printf("Received a message: %s", d.Body)
			time.Sleep(30 * time.Second)
			ch.Ack(d.DeliveryTag, false)
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
