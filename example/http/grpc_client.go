package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	cus_otel "github.com/Loopmaas/otel"
	"github.com/Loopmaas/otel/example/api"
	otelgrpc "github.com/Loopmaas/otel/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type helloClient struct {
	conn *grpc.ClientConn
	api.HelloServiceClient
}

func NewHelloClient(grpcAddr string) (helloClient, error) {

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.TracingMiddleware(otelgrpc.RoleClient)))
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}

	client := api.NewHelloServiceClient(conn)

	return helloClient{conn, client}, nil
}

func (c *helloClient) Close() error {
	return c.conn.Close()
}

func callSayHello(ctx context.Context, c api.HelloServiceClient) error {
	ctx, span := cus_otel.StartTrace(ctx)
	defer span.End()

	response, err := c.SayHello(ctx, &api.HelloRequest{Greeting: "World"})
	if err != nil {
		return fmt.Errorf("calling SayHello: %w", err)
	}
	cus_otel.Info(ctx, "Response from server", cus_otel.NewField("reply", response.Reply))
	return nil
}

func callSayHelloClientStream(ctx context.Context, c api.HelloServiceClient) error {
	ctx, span := cus_otel.StartTrace(ctx)
	defer span.End()

	stream, err := c.SayHelloClientStream(ctx)
	if err != nil {
		return fmt.Errorf("opening SayHelloClientStream: %w", err)
	}

	for i := 0; i < 5; i++ {
		err := stream.Send(&api.HelloRequest{Greeting: "World"})

		time.Sleep(time.Duration(i*50) * time.Millisecond)

		if err != nil {
			return fmt.Errorf("sending to SayHelloClientStream: %w", err)
		}
	}

	response, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("closing SayHelloClientStream: %w", err)
	}

	cus_otel.Info(ctx, fmt.Sprintf("Response from server: %s", response.Reply))
	return nil
}

func callSayHelloServerStream(ctx context.Context, c api.HelloServiceClient) error {
	ctx, span := cus_otel.StartTrace(ctx)
	defer span.End()

	stream, err := c.SayHelloServerStream(ctx, &api.HelloRequest{Greeting: "World"})
	if err != nil {
		return fmt.Errorf("opening SayHelloServerStream: %w", err)
	}

	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("receiving from SayHelloServerStream: %w", err)
		}

		cus_otel.Info(ctx, fmt.Sprintf("Response from server: %s", response.Reply))
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func callSayHelloBidiStream(ctx context.Context, c api.HelloServiceClient) error {
	ctx, span := cus_otel.StartTrace(ctx)
	defer span.End()

	stream, err := c.SayHelloBidiStream(ctx)
	if err != nil {
		return fmt.Errorf("opening SayHelloBidiStream: %w", err)
	}

	serverClosed := make(chan struct{})
	clientClosed := make(chan struct{})

	go func() {
		for i := 0; i < 5; i++ {
			err := stream.Send(&api.HelloRequest{Greeting: "World"})
			if err != nil {
				// nolint: revive  // This acts as its own main func.
				log.Fatalf("Error when sending to SayHelloBidiStream: %s", err)
			}

			time.Sleep(50 * time.Millisecond)
		}

		err := stream.CloseSend()
		if err != nil {
			cus_otel.Error(ctx, "closing SayHelloBidiStream", cus_otel.NewField("error", err))
		}

		clientClosed <- struct{}{}
	}()

	go func() {
		for {
			response, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				cus_otel.Error(ctx, "receiving from SayHelloBidiStream", cus_otel.NewField("error", err))
			}

			log.Printf("Response from server: %s", response.Reply)
			time.Sleep(50 * time.Millisecond)
		}

		serverClosed <- struct{}{}
	}()

	<-clientClosed
	<-serverClosed
	return nil
}
