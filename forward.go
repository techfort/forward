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
	Channel() (EventChannel, chan error)
	Close()
}

// Key strips off the keyspace prefix
func Key(key string) string {
	return strings.Replace(key, "__keyspace@0__:", "", 1)
}

// RedisKV wraps the result of a fetch
type RedisKV struct {
	Key  string
	Data string
	Type string
}

func (kv RedisKV) String() (string, error) {
	var i interface{}
	err := json.Unmarshal([]byte(kv.Data), &i)
	if err != nil {
		return "", errors.Wrap(err, "failed to unmarshal Data of event")
	}
	return fmt.Sprintf(`{"Key": "%v", "Type": "%v", "Data": "%v"}`, kv.Key, kv.Type, i.(string)), err
}

// EventChannel is an alias for a channel of rediskv events
type EventChannel chan RedisKV

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
func (ps pubsub) Channel() (EventChannel, chan error) {
	fmt.Println("Starting Forward event listener...")
	events := make(EventChannel, 2)
	errs := make(chan error, 2)
	go func() {
		msgs := ps.PubSub.Channel()
		for msg := range msgs {
			op, key := msg.Payload, Key(msg.Channel)
			go func() {
				event, err := ps.Conn.Fetch(op, key)
				if err != nil {
					errs <- errors.Wrap(err, "failed to fetch redis key-value pair")
				} else {
					events <- event
				}
			}()
		}
	}()
	return events, errs
}

func (ps pubsub) Close() {
	ps.Conn.Close()
	ps.Close()
}

func (c conn) del(key string) (RedisKV, error) {
	return RedisKV{key, "", "delete"}, nil
}

func (c conn) fetchString(key string) (RedisKV, error) {
	s, err := c.Get(key).Result()
	return RedisKV{key, s, "string"}, err
}

func (c conn) fetchHash(key string) (RedisKV, error) {
	h, err := c.HGetAll(key).Result()
	if err != nil {
		return RedisKV{}, err
	}
	data, err := json.Marshal(h)
	return RedisKV{key, string(data), "hash"}, err
}

func (c conn) fetchSet(key string) (RedisKV, error) {
	s, err := c.SMembers(key).Result()
	if err != nil {
		return RedisKV{}, err
	}
	data, err := json.Marshal(s)
	return RedisKV{key, string(data), "set"}, err
}

func (c conn) fetchZSet(key string) (RedisKV, error) {
	z, err := c.ZRange(key, 0, -1).Result()
	if err != nil {
		return RedisKV{}, err
	}
	data, err := json.Marshal(z)
	return RedisKV{key, string(data), "zset"}, err
}

func (c conn) fetchList(key string) (RedisKV, error) {
	l, err := c.LRange(key, 0, -1).Result()
	if err != nil {
		return RedisKV{}, err
	}
	data, err := json.Marshal(l)
	return RedisKV{key, string(data), "list"}, err
}

func (c conn) expire(key string) (RedisKV, error) {
	return RedisKV{key, "", "expire"}, nil
}

func (c conn) expired(key string) (RedisKV, error) {
	return RedisKV{key, "", "expired"}, nil
}

func (c conn) renameFrom(key string) (RedisKV, error) {
	return RedisKV{key, "", "rename_from"}, nil
}

func (c conn) renameTo(key string) (RedisKV, error) {
	return RedisKV{key, "", "rename_to"}, nil
}

// Handler is a function type to fetch keys
type Handler func(string) (RedisKV, error)

func actionMap(c conn) map[string]Handler {
	return map[string]Handler{
		"expire":       c.expire,
		"expired":      c.expired,
		"rename_to":    c.renameTo,
		"rename_from":  c.renameFrom,
		"sadd":         c.fetchSet,
		"srem":         c.fetchSet,
		"spop":         c.fetchSet,
		"sinterstore":  c.fetchSet,
		"sunionstore":  c.fetchSet,
		"sdiffstore":   c.fetchSet,
		"zadd":         c.fetchZSet,
		"zrem":         c.fetchZSet,
		"zrembyscore":  c.fetchZSet,
		"zrembyrank":   c.fetchZSet,
		"znterstore":   c.fetchZSet,
		"zdiffstore":   c.fetchZSet,
		"zunionstore":  c.fetchZSet,
		"set":          c.fetchString,
		"append":       c.fetchString,
		"setrange":     c.fetchString,
		"incrby":       c.fetchString,
		"incrbyfloat":  c.fetchString,
		"incr":         c.fetchString,
		"decrby":       c.fetchString,
		"decr":         c.fetchString,
		"lpush":        c.fetchList,
		"sortstore":    c.fetchList,
		"lpop":         c.fetchList,
		"linsert":      c.fetchList,
		"blpop":        c.fetchList,
		"brpop":        c.fetchList,
		"rpop":         c.fetchList,
		"rpush":        c.fetchList,
		"lpushx":       c.fetchList,
		"rpushx":       c.fetchList,
		"ltrim":        c.fetchList,
		"lpoprpush":    c.fetchList,
		"lset":         c.fetchList,
		"hset":         c.fetchHash,
		"hincrby":      c.fetchHash,
		"hincrbyfloat": c.fetchHash,
		"hmset":        c.fetchHash,
		"hsetnx":       c.fetchHash,
		"del":          c.del,
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
