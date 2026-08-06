generate:
	@echo "Generating..."
	make mocks


mocks:
	go install go.uber.org/mock/mockgen@v0.5.0
	go generate ./...


# Everything that needs no broker. Fast, and safe to run anywhere.
test:
	go test -race ./...

# Per-driver integration suites. Each brings up only its own broker; the
# matching integration_* build tag is what pulls those test files in at all,
# so `make test` above never touches them.
test-kafka:
	docker compose up -d --wait broker
	go test -v -tags integration_kafka ./...

test-nats:
	docker compose up -d --wait nats
	go test -v -tags integration_nats ./...

test-pulsar:
	docker compose up -d --wait pulsar
	go test -v -tags integration_pulsar ./...

test-all: test test-kafka test-nats test-pulsar

.PHONY: generate mocks test test-kafka test-nats test-pulsar test-all
