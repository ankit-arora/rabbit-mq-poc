package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type rpcClient struct {
	sync.Mutex
	correlationIDs map[string]bool
	replyQueue     amqp.Queue
	msgs           <-chan amqp.Delivery
	ch             *amqp.Channel
}

func newRPCClient(ch *amqp.Channel) *rpcClient {
	q, err := ch.QueueDeclare(
		"",    // name
		true,  // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		true,   // exclusive
		false,  // no-local
		false,  // no-wait
		nil,
	)
	r := &rpcClient{correlationIDs: make(map[string]bool), replyQueue: q, msgs: msgs, ch: ch}
	go func(r *rpcClient) {
		for d := range msgs {
			r.Lock()
			if _, ok := r.correlationIDs[d.CorrelationId]; ok {
				log.Printf("Received a message with id: %s , message: %s", d.CorrelationId, d.Body)
			}
			delete(r.correlationIDs, d.CorrelationId)
			r.Unlock()
		}
	}(r)
	return r
}

func (r *rpcClient) call(body, serverQueueName string) {
	correlationID := uuid.NewString()
	//body := "Hello World!"
	r.Lock()
	r.correlationIDs[correlationID] = true
	r.Unlock()
	q, err := r.ch.QueueDeclare(
		serverQueueName, // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")
	err = r.ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			Body:          []byte(body),
			DeliveryMode:  2,
			CorrelationId: correlationID,
			ReplyTo:       r.replyQueue.Name,
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent a message with id: %s : %s", correlationID, body)
}

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
	forever := make(chan bool)
	err = ch.Confirm(false)
	r := newRPCClient(ch)
	for i := 0; i < 10; i++ {
		r.call(fmt.Sprintf("hello %d", i), "rpc-server-queue")
	}
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
