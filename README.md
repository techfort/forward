# forward

`forward` is a micro Go library that enables keyspace notifications on redis and listens to events, retrieves newly added/updated data and makes it available for handling or forwarding to other data channels (such as messaging systems or streams). Deletion and expiration events return the relevant key.

Checkout `example/forwardtorabbitmq.go` to see how data from redis is forwarded onto rabbitmq queues and then consumed.


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
  //... your logic here...
}
```
## Options to Config

`Addr`: address of the redis server/cluster  
`Pattern`: string pattern for matching only keys containing the pattern and discarding the rest  
`KeyspaceID`: the keyspace ID to subscribe to, but be mindful that redis clusters only allow ID 0, so you can omit this.
