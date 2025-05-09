package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	cus_otel "github.com/Loopmaas/otel"
	"github.com/Loopmaas/otel/example/api"

	"log"
	"net"
	"time"

	otelgrpc "github.com/Loopmaas/otel/grpc"

	"google.golang.org/grpc"
)

var (
	_grpcServerName = "cus_otel-grpc-example"
	_grpcHost       = "localhost"
	_grpcPort       = "7777"
	_otelUrl        = "localhost:43177" // Change this to your otlp collector address
)

type server struct {
	api.HelloServiceServer
}

// SayHello implements api.HelloServiceServer.
func (s *server) SayHello(ctx context.Context, in *api.HelloRequest) (*api.HelloResponse, error) {
	// Start a trace
	ctx, span := cus_otel.StartTrace(ctx)
	defer span.End()

	cus_otel.Info(ctx, "SayHello", cus_otel.NewField("greeting", in.Greeting))

	// Simulate some work
	s.doSomething(ctx)
	time.Sleep(50 * time.Millisecond)

	return &api.HelloResponse{Reply: "Hello " + in.Greeting}, nil
}

func (s *server) doSomething(ctx context.Context) {
	// Start a trace
	ctx, span := cus_otel.StartTrace(ctx)
	defer span.End()

	// Simulate some work
	time.Sleep(50 * time.Millisecond)
	cus_otel.Info(ctx, "doSomething", cus_otel.NewField("key", "value"))
}

func (s *server) SayHelloServerStream(in *api.HelloRequest, out api.HelloService_SayHelloServerStreamServer) error {
	// Start a trace
	ctx, span := cus_otel.StartTrace(context.Background())
	defer span.End()

	// Simulate some streaming work
	for i := 0; i < 5; i++ {
		err := out.Send(&api.HelloResponse{Reply: "Hello " + in.Greeting})
		if err != nil {
			return err
		}

		time.Sleep(time.Duration(i*50) * time.Millisecond)
	}

	s.doSomething(ctx)

	return nil
}

func (s *server) SayHelloClientStream(stream api.HelloService_SayHelloClientStreamServer) error {
	// Start a trace
	ctx, span := cus_otel.StartTrace(context.Background())
	defer span.End()
	i := 0

	for {
		in, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			cus_otel.Error(ctx, "SayHelloClientStream", cus_otel.NewField("error", err))
			return err
		}

		cus_otel.Info(ctx, "SayHelloClientStream", cus_otel.NewField("greeting", in.Greeting))
		i++
	}

	time.Sleep(50 * time.Millisecond)

	return stream.SendAndClose(&api.HelloResponse{Reply: fmt.Sprintf("Hello (%v times)", i)})
}

func (s *server) SayHelloBidiStream(stream api.HelloService_SayHelloBidiStreamServer) error {
	// Start a trace
	ctx, span := cus_otel.StartTrace(context.Background())
	defer span.End()

	for {
		in, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			cus_otel.Error(ctx, "SayHelloBidiStream", cus_otel.NewField("error", err))
			return err
		}

		time.Sleep(50 * time.Millisecond)

		cus_otel.Info(ctx, "SayHelloBidiStream", cus_otel.NewField("greeting", in.Greeting))
		err = stream.Send(&api.HelloResponse{Reply: "Hello " + in.Greeting})
		if err != nil {
			cus_otel.Error(ctx, "SayHelloBidiStream", cus_otel.NewField("error", err))
			return err
		}
	}

	return nil
}

func startGrpcServer(ctx context.Context) {
	shutdown, err := cus_otel.InitTelemetry(ctx, _grpcServerName, _otelUrl)
	if err != nil {
		log.Fatal(err)
	}

	// Graceful shutdown
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", _grpcHost, _grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.TracingMiddleware(otelgrpc.RoleServer)),
	)

	go func() {
		api.RegisterHelloServiceServer(s, &server{})
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	log.Println("gRPC server started...")

	<-ctx.Done()

	log.Println("gRPC server shut down gracefully...")
}
