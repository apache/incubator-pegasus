# pegasus-go-client

This is the official go client for [xiaomi/pegasus](https://github.com/XiaoMi/pegasus).

## Installation

Go1.7+

As we use [glide](https://github.com/Masterminds/glide) as the vendoring tool,
it's recommended to install glide before you build this package.

For those who wants to use `go get` to install the package, please make sure your
thrift lib is in version **0.9.3**.

## Example

```go
    import (
        "context"
        
    	"github.com/XiaoMi/pegasus-go-client/pegasus"
    )
    
    cfg := Config{
        MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
    }
    
    client := NewClient(cfg)
    defer client.Close()
    
    tb, err := client.OpenTable(context.Background(), "temp")
    err = tb.Set(context.Background(), []byte("h1"), []byte("s1"), []byte("v1"))
```
