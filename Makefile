fmt:
	go fmt ./...

ci:
	golangci-lint run -c .golangci.yml --timeout 5m0s
