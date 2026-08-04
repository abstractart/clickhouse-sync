# Build the mover and the filesystem-cache demo as static binaries.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/clickhouse-sync ./cmd/mover \
 && CGO_ENABLED=0 go build -o /out/fscache ./cmd/fscache

FROM alpine:3.20
COPY --from=build /out/clickhouse-sync /usr/local/bin/clickhouse-sync
COPY --from=build /out/fscache /usr/local/bin/fscache
# Default entrypoint is the mover; the fscache compose service overrides it.
ENTRYPOINT ["clickhouse-sync"]
