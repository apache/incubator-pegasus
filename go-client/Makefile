#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

build:
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/backup.thrift
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/bulk_load.thrift
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/dsn.layer2.thrift
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/duplication.thrift
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/meta_admin.thrift
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/metadata.thrift
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/partition_split.thrift
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/replica_admin.thrift
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/rrdb.thrift
	thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/go-client/idl/' ../idl/command.thrift
	rm -Rf idl/admin/admin_client-remote
	rm -Rf idl/cmd/remote_cmd_service-remote
	rm -Rf idl/radmin/replica_client-remote
	go mod tidy
	go mod verify
	go build -o ./bin/echo ./rpc/main/echo.go
	go build -o ./bin/generator ./generator/main.go
	./bin/generator -i ./generator/admin.csv -t admin > ./session/admin_rpc_types.go
	./bin/generator -i ./generator/radmin.csv -t radmin > ./session/radmin_rpc_types.go
	go build -o ./bin/example ./example/main.go
	go build -o ./bin/failover-test ./integration/failover-test/main.go

fmt:
	go fmt ./...

ci:
	go test -race -v -failfast -test.timeout 5m -coverprofile=coverage.txt -covermode=atomic ./...
