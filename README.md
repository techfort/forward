# forward

`forward` is a micro Go library that enables keyspace notifications on redis and listens to events, wraps them in a struct containing `Data`, `Key` and `Type` and makes them available for processing (eg. forwarding from Redis to RabbitMQ, Kinesis, Kafka etc.) Deletion and expiration events behave the same way, except the `Data` field is `nil`.

Checkout `example/forwardtorabbitmq.go` to see how data from redis is forwarded onto rabbitmq queues and then consumed.

Docs: https://godoc.org/github.com/techfort/forward

## Usage

`NewPubSub` returns a pub sub object
The `Channel()` methods returns an array of events containing the data, the type and the key of the redis data affected by an event.

To use `forward` you only need a few lines:
```
ps := forward.NewPubSub(forward.PubSubConfig{
		Addr:       v.GetString("REDIS_URL"),
		Pattern:    "*",
		KeyspaceID: 0,
})

events, errs := ps.Channel()

for e := range events {
  /*
  Example
  The following Redis operation: SET foo bar
  would generate an event which is a forward.RedisKV{Data:"bar", Key: "foo", Type: "string"}
  */
  //... your logic here...
}
```
## Options to Config

`Addr`: address of the redis server/cluster  
`Pattern`: string pattern for matching only keys containing the pattern and discarding the rest  
`KeyspaceID`: the keyspace ID to subscribe to, but be mindful that redis clusters only allow ID 0, so you can omit this.
