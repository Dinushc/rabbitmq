.PHONY: build test race vet fmt integration

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

integration:
	RABBITMQ_URL=$${RABBITMQ_URL:-amqp://guest:guest@localhost:5672/} \
		go test ./client -run Integration -v -count=1
