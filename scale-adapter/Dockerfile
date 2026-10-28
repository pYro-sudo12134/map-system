FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o adapter .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/adapter /adapter
USER 1000
EXPOSE 8080
ENTRYPOINT ["/adapter"]