build:
	go mod tidy
	go mod verify
	go build -o ./bin/echo ./rpc/main/echo.go
	go build -o ./bin/failover-test ./integration/failover-test/main.go
	go build -o ./bin/generator ./generator/main.go
	./bin/generator -i ./generator/admin.csv -t admin > ./session/admin_rpc_types.go
	./bin/generator -i ./generator/radmin.csv -t radmin > ./session/radmin_rpc_types.go
	go build -o ./bin/example ./example/main.go

fmt:
	go fmt ./...

ci:
	go test -race -v -failfast -test.timeout 5m -coverprofile=coverage.txt -covermode=atomic ./...
