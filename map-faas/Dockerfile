FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o handler .

FROM alpine:latest

COPY --from=builder /app/handler /handler

ENV fprocess="/handler"

CMD ["fwatchdog"]