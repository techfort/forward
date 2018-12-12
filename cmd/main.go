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
	err := make(chan error, 2)
	for e := range ps.Channel() {
		fmt.Println(fmt.Sprintf("Event: %+v", e))
	}
	<-err
}
