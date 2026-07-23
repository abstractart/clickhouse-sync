COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: build test up schema load init down clean demo move show client bucket truncate help

## build the mover binary
build:
	go build -o clickhouse-sync .

## run unit tests
test:
	go test ./...

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
