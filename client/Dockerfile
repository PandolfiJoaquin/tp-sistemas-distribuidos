# Build stage
FROM golang:1.23.0-alpine3.20 AS builder

WORKDIR /home/app

COPY /client/go.mod /client/go.sum ./
RUN go mod download

COPY /client ./

RUN go build -o analyzer-client ./main.go

# # Test stage
# FROM builder AS client-test-stage

# # CMD ["go", "test", "-v", "./tests"]
# COPY /client/rsc/client.png ./tests/rsc/client.png
# CMD ["sh", "-c", "go test -cover -coverprofile=coverage/coverage.tmp.out $(go list ./... | grep -v 'src/auth') && \
# cat coverage/coverage.tmp.out | grep -v -e 'main.go' -e 'rabbit_messages.go' -e 'send_email.go' > coverage/coverage.out"]

# Run stage
FROM alpine:3.20

WORKDIR /home/app

COPY --from=builder /home/app/analyzer-client ./

ENTRYPOINT ["./analyzer-client"]
