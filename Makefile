build:
	go mod tidy
	go mod verify
	go build -o ./bin/example ./example/main.go
	go build -o ./bin/echo ./rpc/main/echo.go

fmt:
	go fmt ./...

ci:
	golangci-lint run -c .golangci.yml --timeout 5m0s
	go test -race -v -test.timeout 1m ./...
