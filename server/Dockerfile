FROM golang:1.23-alpine as builder

WORKDIR /build

ARG NODE

COPY server/go.mod .
COPY server/go.sum .

RUN go mod download

COPY server/common/ common/
COPY server/${NODE}/ ${NODE}/

RUN go build -o /build/app tp-sistemas-distribuidos/server/${NODE}

FROM busybox:latest
LABEL app="imdb"

COPY --from=builder /build/app /

ENTRYPOINT ["./app"]


