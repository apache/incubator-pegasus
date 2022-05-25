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
thrift -I idl -out idl --gen go:thrift_import='github.com/pegasus-kv/thrift/lib/go/thrift',package_prefix='github.com/XiaoMi/pegasus-go-client/idl/' idl/admin.thrift
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
