# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /home/app

COPY client/. ./client/
COPY pkg/ ./pkg/

WORKDIR /home/app/client

RUN go mod tidy
RUN go build -o client_bin 


# Run stage
FROM busybox:1.32.1

WORKDIR /home/app

COPY --from=builder /home/app/client/client_bin ./

ENTRYPOINT ["./client_bin"]
