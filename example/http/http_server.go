package main

import (
	"context"
	"log"
	"net/http"

	cus_otel "github.com/Loopmaas/otel"
	otelgin "github.com/Loopmaas/otel/gin"

	"github.com/gin-gonic/gin"
)

var (
	_httpServiceName = "cus_otel-http-example"
	_httpHost        = "localhost"
	_httpPort        = "8080"
	_otelUrl         = "localhost:43177" // Change this to your otlp collector address
	_grpcUrl         = "localhost:7777"
)

func StartHttpServer(ctx context.Context) {
	// Initialize telemetry
	shutdown, err := cus_otel.InitTelemetry(ctx, _httpServiceName, _otelUrl)
	if err != nil {
		log.Fatal(err)
	}

	// Graceful shutdown
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	// Initialize the gRPC client
	helloClient, err := NewHelloClient(_grpcUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer helloClient.Close()

	r := gin.New()
	r.Use(otelgin.TracingMiddleware(_httpServiceName))

	r.GET("/version", func(c *gin.Context) {
		ctx, span := cus_otel.StartTrace(c.Request.Context())
		defer span.End()

		foo(ctx)

		cus_otel.Info(ctx, "version endpoint called")
		c.JSON(200, gin.H{
			"version": "0.1.0",
		})
	})

	r.GET("/hello", func(c *gin.Context) {
		ctx, span := cus_otel.StartTrace(c.Request.Context())
		defer span.End()

		_ = callSayHello(ctx, helloClient.HelloServiceClient)
	})

	r.GET("/helloClientStream", func(c *gin.Context) {
		ctx, span := cus_otel.StartTrace(c.Request.Context())
		defer span.End()

		_ = callSayHelloClientStream(ctx, helloClient.HelloServiceClient)
	})

	r.GET("/helloServerStream", func(c *gin.Context) {
		ctx, span := cus_otel.StartTrace(c.Request.Context())
		defer span.End()

		_ = callSayHelloServerStream(ctx, helloClient.HelloServiceClient)
	})

	r.GET("/helloBidiStream", func(c *gin.Context) {
		ctx, span := cus_otel.StartTrace(c.Request.Context())
		defer span.End()

		_ = callSayHelloBidiStream(ctx, helloClient.HelloServiceClient)
	})

	srv := &http.Server{
		Addr:    _httpHost + ":" + _httpPort,
		Handler: r,
	}

	// Initializing the server in a goroutine so that
	// it won't block the graceful shutdown handling below
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// Listen for the interrupt signal.
	<-ctx.Done()

	log.Println("Http server shut down gracefully...")
}

// foo is a sample function that calls bar.
func foo(ctx context.Context) {
	ctx, span := cus_otel.StartTrace(ctx)
	defer span.End()

	cus_otel.Warn(ctx, "foo called")

	bar(ctx)
}

// bar is a sample function that logs an error.
func bar(ctx context.Context) {
	ctx, span := cus_otel.StartTrace(ctx)
	defer span.End()

	cus_otel.Error(ctx, "bar called")
}
