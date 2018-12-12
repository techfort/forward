package main

import (
	"fmt"

	"forward"

	"github.com/spf13/viper"
)

func main() {
	v := viper.New()
	v.AutomaticEnv()
	ps := forward.NewPubSub(forward.PubSubConfig{
		Addr:       v.GetString("PP_REDIS_URL"),
		Pattern:    "*",
		KeyspaceID: 0,
	})
	defer ps.Close()
	events, errs := ps.Channel()
	go func() {
		for e := range events {
			fmt.Println(fmt.Sprintf("Event: %+v", e))
		}
	}()
	<-errs
}
