package main

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"forward"

	"github.com/spf13/viper"
)

/*
example of kicking off the forward
*/

func main() {
	v := viper.New()
	v.AutomaticEnv()
	ps := forward.NewPubSub(forward.PubSubConfig{
		Addr:       v.GetString("PP_REDIS_URL"),
		Pattern:    "*",
		KeyspaceID: 0,
	})
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(errors.Wrap(err, "cannot dial rabbitmq connection"))
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		panic(errors.Wrap(err, "cannot create rabbitmq channel"))
	}
	q, err := ch.QueueDeclare(
		"forwarded-events",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(errors.Wrap(err, "cannot create queue"))
	}
	defer ps.Close()
	events, errs := ps.Channel()
	go func() {
		for e := range events {
			bytes, err := json.Marshal(e)
			if err != nil {
				errs <- errors.Wrap(err, "cannot marshal Redis event")
				continue
			}
			fmt.Println(fmt.Sprintf("Event: %+v", e))
			ch.Publish("",
				q.Name,
				false,
				false,
				amqp.Publishing{
					ContentType: "application/json",
					Body:        bytes,
				},
			)
		}
	}()
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err, ok := r.(error)
				if !ok {
					err = fmt.Errorf("some error occurred")
				}
				fmt.Println(err.Error())
			}
		}()
		fmt.Println("listening on rabbit...")
		for m := range msgs {
			var msg map[string]interface{}
			err := json.Unmarshal(m.Body, &msg)
			if err != nil {
				panic(errors.Wrap(err, "cannot unmarshal delivery"))
			}
			re := forward.RedisKV{
				Data: msg["Data"].(string),
				Key:  msg["Key"].(string),
				Type: msg["Type"].(string),
			}
			fmt.Println(fmt.Sprintf("Consumed by RabbitMQ: %+v", re))
		}
	}()
	<-errs
}
