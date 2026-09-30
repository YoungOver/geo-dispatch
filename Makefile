.PHONY: run test bench sim flood
run:
	go run ./cmd/server
test:
	go test -race ./...
bench:
	go test -run x -bench . -benchmem ./internal/...
sim:
	go run ./cmd/sim -couriers 200000 -interval 1s -queriers 64 -d 20s
flood:
	go run ./cmd/sim -couriers 200000 -queriers 1 -d 10s
