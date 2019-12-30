# pegasus-go-client

This is the official go client for [xiaomi/pegasus](https://github.com/XiaoMi/pegasus).

## Requirement

Go1.12+.

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

For more examples please refer to [example/](example/main.go).
