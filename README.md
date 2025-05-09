# Cus Otel

This is an integration tool based on opentelemetry-go, primarily implementing tracing functionality in gin and grpc.
It allows developers to use simple methods to achieve service monitoring and logging functions during development.

You can refer to the [`example`] to implement the functionality.

# Usage

Below is a simple example showing how to initialize telemetry and how to add middleware to a gin server:

```go
import (
	"context"
	cus_otel "cus/otel"
	otelgin "cus/otel/gin"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

    // Initialize telemetry
	shutdown, err := cus_otel.InitTelemetry(ctx, "service_name", "otel_collector_url")
	if err != nil {
		log.Fatal(err)
	}

	// Graceful shutdown
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}()

    // Start a gin server
    r := gin.New()
	r.Use(otelgin.TracingMiddleware(_httpServiceName))

	r.GET("/", func(c *gin.Context) {
        // Start a span under the `/version`
		ctx, span := cus_otel.StartTrace(c.Request.Context())
		defer span.End()

        // Log some messages
		cus_otel.Info(ctx, "Hello world!")
        cus_otel.Warn(ctx,"Oops~")
        cus_otel.Error(ctx,"Oh No!")

		c.JSON(200, gin.H{
			"msg": "Hello world!",
		})
	})

    // ...
}
```