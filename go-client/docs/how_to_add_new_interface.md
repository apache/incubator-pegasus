<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Howto: Add New RPC Interface

This document is lastly edited by Tao Wu (wutao@apache.org), 2021/4/28.

This document illustrates the steps of adding a new RPC interface to the client.

## Steps

1. Add the RPC's thrift definitions to `idl/`.

You can write definitions to a new file, for example, foo.thrift.
Or you can just write them to existing files. It depends on which module that the new interface belongs to.
Currently, `idl/admin.thrift` contains all MetaServer administration RPCs,
`idl/radmin.thrift` contains the RPCs all ReplicaServer administration RPCs.

2. Prepare the thrift compiler.

pegasus-go-client uses thrift-compiler with 0.13.0, which is the version you can directly install via `apt-get` on Ubuntu 20.04.
If the compiler is missing on your OS,
you can follow [the building manual of thrift](https://thrift.apache.org/docs/BuildingFromSource) and build it yourself.

3. Compile thrift files to generate go code.

```sh
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/backup.thrift
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/bulk_load.thrift
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/dsn.layer2.thrift
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/dsn.thrift
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/duplication.thrift
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/meta_admin.thrift
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/metadata.thrift
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/partition_split.thrift
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/replica_admin.thrift
thrift -I ../idl -out idl --gen go:thrift_import='github.com/apache/thrift/lib/go/thrift',package_prefix='github.com/apache/incubator-pegasus/idl/' ../idl/rrdb.thrift
gofmt -w **/*.go
goimports -w */**.go
```

4. Add functions to `session/meta_session.go` or `session/replica_session.go` for the new RPC.

In this step no hand-writing code is required. You merely need to append a tuple of "TaskCode,RequestType,ResponseType"
to `generator/admin.csv` or `generator/radmin.csv`.
Then run `make` which automatically generates code according to the template.

5. Register the callback for the new RPC.

This step is trivial, and the code can be auto-generated in the future. 
For now, you should go `session/codec.go` and add a new response callback to `nameToResultMap`.
The example code:

```go
"RPC_RRDB_RRDB_INCR_ACK": func() RpcResponseResult {
    return &rrdb.RrdbIncrResult{
        Success: rrdb.NewIncrResponse(),
    }
},
```
