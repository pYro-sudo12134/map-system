FROM ghcr.io/openfaas/of-watchdog:0.9.11 AS watchdog
FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -o handler \
    .

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

COPY --from=watchdog /fwatchdog /usr/bin/fwatchdog
COPY --from=builder /app/handler /handler

RUN chmod +x /handler /usr/bin/fwatchdog

ENV fprocess="/handler"
ENV mode="streaming"
ENV upstream_url="http://127.0.0.1:8082"
ENV cgi_headers="true"
ENV read_timeout="300s"
ENV write_timeout="300s"
ENV exec_timeout="290s"

EXPOSE 8080

USER 1000

CMD ["fwatchdog"]