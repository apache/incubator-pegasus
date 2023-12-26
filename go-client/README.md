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
# pegasus-go-client

[![Go Report Card](https://goreportcard.com/badge/github.com/apache/incubator-pegasus/go-client)](https://goreportcard.com/report/github.com/apache/incubator-pegasus/go-client)

## Requirement

- Go1.18+
- thrift 0.13

## Development

Build the pegasus-go-client:
```bash
make build
```

Format the code:
```bash
make fmt
```

It requires the Pegasus [onebox](https://pegasus.apache.org/overview/onebox/) has been started.

Then run tests:
```bash
make ci
```

## Logging

By default, pegasus-go-client logs to "./pegasus.log" on where your application runs.
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

        "github.com/apache/incubator-pegasus/go-client/pegasus"
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
