package main

import (
	pb "github.com/ElioenaiFerrari/rhino/gen/go"
)

func main() {
	publishRequest := pb.PublishRequest{
		Topic:      "topic",
		Data:       []byte("data"),
		Ttl:        1000,
		Persistent: true,
	}

	subscriptionRequest := pb.SubscriptionRequest{
		Topic: "topic",
	}

	rhinov1connect.Publish(&publishRequest)

}
