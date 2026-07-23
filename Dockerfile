# Build the mover as a static binary.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/clickhouse-sync .

FROM alpine:3.20
COPY --from=build /out/clickhouse-sync /usr/local/bin/clickhouse-sync
ENTRYPOINT ["clickhouse-sync"]
