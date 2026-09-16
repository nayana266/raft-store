.PHONY: test race proto run-dev

test:
	go test ./...

race:
	go test ./... -race -count=1

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/raftkv.proto

run-dev:
	go run ./cmd -dev
