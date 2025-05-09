package cus_otel

import (
	"context"

	"github.com/go-resty/resty/v2"
)

func TraceRestyResponse(ctx context.Context, msg string, url string, resp *resty.Response) {
	ctx, span := StartTrace(ctx)
	defer span.End()

	traceInfo := resp.Request.TraceInfo()

	var remoteAddr string
	if traceInfo.RemoteAddr != nil {
		remoteAddr = traceInfo.RemoteAddr.String()
	}

	Info(ctx, msg,
		NewField("url", url),
		NewField("status_code", resp.StatusCode()),
		NewField("time", resp.Time()),
		NewField("received_at", resp.ReceivedAt()),
		NewField("response_body_size", resp.Header().Get("Content-Length")),
		NewField("remote_addr", remoteAddr),
		NewField("connection_time", traceInfo.ConnTime),
		NewField("request_attempt", traceInfo.RequestAttempt),
		NewField("response_time", traceInfo.ResponseTime),
		NewField("server_time", traceInfo.ServerTime),
		NewField("total_time", traceInfo.TotalTime),
	)
}
