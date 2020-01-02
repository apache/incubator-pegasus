# pegasus-go-client

This is the official go client for [xiaomi/pegasus](https://github.com/XiaoMi/pegasus).

## Requirement

Go1.12+.

## Logging

By default pegasus-go-client logs to "./pegasus.log" on where your application runs.
You can customize the logging rules as follows:

```go
    // customize where the pegasus-go-client's logs reside.
    pegalog.SetLogger(pegalog.NewLogrusLogger(&pegalog.LogrusConfig{
        // rotation rules
        MaxSize:    500, // megabytes
        MaxAge:     5,   // days
        MaxBackups: 100,
        // log files location
        Filename:   "/home/work/myapp/log/pegasus.log",
    }))
```

To print the logs on screen:

```go
pegalog.SetLogger(pegalog.StderrLogger)
```

We highly recommend you to enable client logging for debugging purpose. If you want
support for other log destinations or log formats, please submit an issue for that.

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
