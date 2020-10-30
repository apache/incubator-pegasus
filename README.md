# pegasus-go-client

[![codecov](https://codecov.io/gh/xiaomi/pegasus-go-client/branch/master/graph/badge.svg)](https://codecov.io/gh/xiaomi/pegasus-go-client)

[![Go Report Card](https://goreportcard.com/badge/github.com/XiaoMi/pegasus-go-client)](https://goreportcard.com/report/github.com/XiaoMi/pegasus-go-client)

[![PkgGoDev](https://pkg.go.dev/badge/github.com/xiaomi/pegasus-go-client)](https://pkg.go.dev/github.com/xiaomi/pegasus-go-client)

This is the official go client for [Apache Pegasus](https://github.com/apache/incubator-pegasus).

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

## TroubleShooting

Before using pegasus-go-client, it's recommended to configure GOBACKTRACE so as to
generate coredump while program unexpectedly corrupts.

```sh
ulimit -c unlimited
export GOBACTRACE=crash
```
