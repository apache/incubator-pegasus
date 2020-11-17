build:
	go mod tidy
	go mod verify
	go build -o ./bin/echo ./rpc/main/echo.go
	go build -o ./bin/generator ./generator/main.go
	./bin/generator -i ./generator/admin.csv -t admin > ./session/admin_rpc_types.go
	./bin/generator -i ./generator/radmin.csv -t radmin > ./session/radmin_rpc_types.go
	go build -o ./bin/example ./example/main.go
fmt:
	go fmt ./...

ci:
	go test -race -v -test.timeout 2m -coverprofile=coverage.txt -covermode=atomic ./...
