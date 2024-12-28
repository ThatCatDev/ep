generate:
	@echo "Generating..."
	make mocks


mocks:
	go install go.uber.org/mock/mockgen@v0.5.0
	go generate ./...
