fmt:
	go fmt ./...

ci:
	golangci-lint run -c .golangci.yml
