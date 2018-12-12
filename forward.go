package forward

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// PubSub is a wrapper for the redis pubsub
type PubSub interface {
	Channel() EventChannel
	Close()
}

type conn struct {
	*redis.Client
}

type pubsub struct {
	*redis.PubSub
	Conn conn
}

// Fetcher interface
type Fetcher interface {
	Fetch(op, key string) (RedisKV, error)
	Close()
}

// PubSubConfig holds the config options for Forward
type PubSubConfig struct {
	Addr       string
	Pattern    string
	KeyspaceID int64
}

// NewRedisClient returns a wrapped redis client
func NewRedisClient(r *redis.Client) Fetcher {
	return conn{r}
}

// NewPubSub returns a wrapper inerface
func NewPubSub(config PubSubConfig) PubSub {
	r := redis.NewClient(&redis.Options{
		Addr: config.Addr,
	})
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}
	_, err = r.ConfigSet("notify-keyspace-events", "KA").Result()
	if err != nil {
		panic(errors.Wrap(err, "unable to turn notifications on the server"))
	}
	pattern := fmt.Sprintf(`__keyspace@%v__:%v`, config.KeyspaceID, config.Pattern)
	return pubsub{r.PSubscribe(pattern), conn{r}}
}

// Channel returns the channel of events
func (ps pubsub) Channel() EventChannel {
	events := make(EventChannel, 2)
	go func() {
		msgs := ps.PubSub.Channel()
		for msg := range msgs {
			op, key := msg.Payload, Key(msg.Channel)
			go func() {
				event, err := ps.Conn.Fetch(op, key)
				if err == nil {
					fmt.Println(fmt.Sprintf("Retrieved: %v => %+v", key, event))
					events <- event
				} else {
					fmt.Println(err)
				}
			}()
		}
	}()
	return events
}

func (ps pubsub) Close() {
	ps.Conn.Close()
	ps.Close()
}

func (c conn) fetchString(key string) (RedisKV, error) {
	s, err := c.Get(key).Result()
	return RedisKV{key, []byte(s), "string"}, err
}

func (c conn) fetchHash(key string) (RedisKV, error) {
	h, err := c.HGetAll(key).Result()
	if err != nil {
		return RedisKV{}, err
	}
	data, err := json.Marshal(h)
	return RedisKV{key, data, "hash"}, err
}

func (c conn) fetchSet(key string) (RedisKV, error) {
	s, err := c.SMembers(key).Result()
	if err != nil {
		return RedisKV{}, err
	}
	data, err := json.Marshal(s)
	return RedisKV{key, data, "set"}, err
}

func (c conn) fetchZSet(key string) (RedisKV, error) {
	z, err := c.ZRange(key, 0, -1).Result()
	if err != nil {
		return RedisKV{}, err
	}
	data, err := json.Marshal(z)
	return RedisKV{key, data, "zset"}, err
}

func (c conn) fetchList(key string) (RedisKV, error) {
	l, err := c.LRange(key, 0, -1).Result()
	if err != nil {
		return RedisKV{}, err
	}
	data, err := json.Marshal(l)
	return RedisKV{key, data, "list"}, err
}

// Handler is a function type to fetch keys
type Handler func(string) (RedisKV, error)

func actionMap(c conn) map[string]Handler {
	return map[string]Handler{
		"set":       c.fetchString,
		"hset":      c.fetchHash,
		"sadd":      c.fetchSet,
		"srem":      c.fetchSet,
		"zadd":      c.fetchZSet,
		"zrem":      c.fetchZSet,
		"lpush":     c.fetchList,
		"lpop":      c.fetchList,
		"hmset":     c.fetchHash,
		"linsert":   c.fetchList,
		"blpop":     c.fetchList,
		"brpop":     c.fetchList,
		"rpop":      c.fetchList,
		"rpush":     c.fetchList,
		"lpushx":    c.fetchList,
		"rpushx":    c.fetchList,
		"ltrim":     c.fetchList,
		"lpoprpush": c.fetchList,
		"lset":      c.fetchList,
		"hincrby":   c.fetchHash,
		"hsetnx":    c.fetchHash,
	}
}

var actions map[string]Handler

func (c conn) Fetch(op, key string) (RedisKV, error) {
	if actions == nil {
		actions = actionMap(c)
	}
	method := actions[op]
	if method == nil {
		return RedisKV{}, fmt.Errorf("could not find a redis action for operation %v", op)
	}
	return method(key)
}

func (c conn) Close() {
	c.Close()
}

// Key strips off the keyspace prefix
func Key(key string) string {
	return strings.Replace(key, "__keyspace@0__:", "", 1)
}

// RedisKV wraps the result of a fetch
type RedisKV struct {
	Key  string
	Data []byte
	Type string
}

// EventChannel is an alias for a channel of rediskv events
type EventChannel chan RedisKV
