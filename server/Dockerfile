FROM golang:1.23-alpine AS builder

WORKDIR /build

ARG NODE

COPY server/go.mod ./server/
COPY server/go.sum ./server/
COPY pkg/ ./pkg/

WORKDIR /build/server

RUN go mod download

COPY server/common/ common/
COPY server/${NODE}/ ${NODE}/

RUN go build -o /build/app tp-sistemas-distribuidos/server/${NODE}

FROM alpine:latest
LABEL app="imdb"

COPY --from=builder /build/app /

ENTRYPOINT ["./app"]


