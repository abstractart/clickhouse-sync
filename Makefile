COMPOSE := docker compose -f deploy/docker-compose.yml

# Packages that count toward coverage: everything except the fscache demo, which
# is a manual research stand with no automated tests.
COVERPKG := $(shell go list ./... | grep -v '/cmd/fscache' | paste -sd, -)

.PHONY: build test test-integration cover cover-html up schema load init down clean demo move show client bucket truncate fscache-demo help

## build the mover binary
build:
	go build -o clickhouse-sync ./cmd/mover

## run fast unit tests (no Docker required)
test:
	go test ./...

## run integration tests against a real ClickHouse via testcontainers (needs Docker)
test-integration:
	go test -tags=integration -timeout 600s -v ./...

## measure statement coverage over the full suite (unit + integration; needs Docker)
cover:
	go test -tags=integration -covermode=atomic -coverpkg=$(COVERPKG) -coverprofile=coverage.out -timeout 600s ./...
	@go tool cover -func=coverage.out | tail -1

## open the HTML coverage report (run `make cover` first)
cover-html:
	go tool cover -html=coverage.out

## start the ClickHouse cluster + SeaweedFS
up:
	$(COMPOSE) up -d --wait

## create the sharded+replicated schema (no data)
schema:
	$(COMPOSE) --profile init run --rm schema

## load demo data into the existing schema (lands on object_storage)
load:
	$(COMPOSE) --profile init run --rm load

## create schema and load data (convenience: schema + load)
init: schema load

## run the mover: move partition 202401 from object_storage to the local disk on every node
move:
	$(COMPOSE) --profile tools run --rm sync \
		-user default -password secret \
		-hostname clickhouse-s1r1 \
		-database demo -table events_local \
		-partition 202401 -partition-id \
		-destination-disk local -insecure

## open an interactive clickhouse-client on a shard #1 node
client:
	$(COMPOSE) --profile tools run --rm client

## show current part placement per node and disk
show:
	$(COMPOSE) --profile tools run --rm show

## list objects in the S3 bucket backing the object_storage disk
bucket:
	$(COMPOSE) --profile tools run --rm bucket

## run the filesystem_cache demo (S3 + local cache; shows cold read vs warm cache hit)
fscache-demo:
	$(COMPOSE) --profile tools run --rm fscache

## fully empty demo.events_local (clean slate; re-run `make init` to reload data)
truncate:
	$(COMPOSE) --profile tools run --rm truncate

## full end-to-end demo: up -> init -> show -> move -> show
demo: up init show move show

## stop containers
down:
	$(COMPOSE) down

## stop containers and remove volumes
clean:
	$(COMPOSE) down -v

help:
	@grep -B1 -E '^[a-z-]+:' Makefile | grep -E '^##|^[a-z-]+:' | sed 's/^## //'
