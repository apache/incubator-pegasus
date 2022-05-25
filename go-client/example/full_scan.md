# Example: full scan

Suppose we have a pegasus table storing user browsing history,
and the requirement is to search for the users that browsed one year ago.
Given a table called `user_history`, which contains data in schema:

```txt
 ----hashkey----  -----sortkey-----    ------value--------
 userId(string) : timestamp(uint64) => web-content(string)
```

So we need to fully scan the table, find the hashkey that contains sortkey
lower than []bytes(oneYearAgoTs).

```go
package main

import (
    "context"
    "encoding/binary"
    "time"

    "github.com/XiaoMi/pegasus-go-client/pegalog"
    "github.com/XiaoMi/pegasus-go-client/pegasus"
)

func searchHistoryOneYearAgo() {
    // Customize where the pegasus-go-client's logs reside.
    pegalog.SetLogger(pegalog.NewLogrusLogger(&pegalog.LogrusConfig{
        Filename: "./pegasus.log",
    }))
    logger := pegalog.GetLogger()

    // Configure the meta addresses to access the pegasus cluster.
    cfg := &pegasus.Config{
        MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34601"},
    }
    c := pegasus.NewClient(*cfg)

    // Establish the connections to replica-servers.
    ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
    tb, err := c.OpenTable(ctx, "user_history")
    if err != nil {
        logger.Print(err)
        return
    }
    logger.Print("opened table user_history")

    // Set up the scanners.
    ctx, _ = context.WithTimeout(context.Background(), time.Second*10)
    sopts := &pegasus.ScannerOptions{
        BatchSize: 20,
        // Values can be optimized out during scanning to reduce the workload.
        NoValue: true,
    }
    scanners, err := tb.GetUnorderedScanners(ctx, 16, sopts)
    if err != nil {
        logger.Print(err)
    }
    logger.Printf("opened %d scanners", len(scanners))
    oneYearAgo := int(time.Now().AddDate(-1, 0, 0).UnixNano() / 1000 / 1000)
    for i, scanner := range scanners {
        // Iterates sequentially.

        start := time.Now()
        cnt := 0
        for true {
            ctx, _ = context.WithTimeout(context.Background(), time.Second*10)
            completed, hashKey, sortKey, _, err := scanner.Next(ctx)
            if err != nil {
                logger.Print(err)
                return
            }
            if completed {
                logger.Printf("scanner %d completes", i)
                break
            }
            if len(sortKey) == 8 {
                res := int(binary.BigEndian.Uint64(sortKey))
                if res < oneYearAgo {
                    logger.Printf("hashkey=%s, sortkey=%d\n", string(hashKey), res)
                }
            }

            cnt++
            if time.Now().Sub(start) > time.Minute {
                logger.Printf("scan 1-min, %d rows in total", cnt)
                start = time.Now()
            }
        }
    }
    logger.Print("program exits")
}
```
