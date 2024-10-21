package main

import (
	pb "github.com/ElioenaiFerrari/rhino/gen/go"
	"google.golang.org/grpc"
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

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	client := pb.NewRhinoClient(conn)

	_, err = client.Publish(nil, &publishRequest)
	if err != nil {
		panic(err)
	}
}
