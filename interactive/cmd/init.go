package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"pegic/executor"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/desertbit/grumble"
)

var globalContext *executor.Context

func Init(metaAddrs []string) error {
	// validate meta addresses
	_, err := session.ResolveMetaAddr(metaAddrs)
	if err != nil {
		return err
	}

	globalContext = executor.NewContext(os.Stdout, metaAddrs)

	// Prints cluster name
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := globalContext.Meta.QueryClusterInfo(ctx, &admin.ClusterInfoRequest{})
	if err != nil {
		if err == context.DeadlineExceeded {
			fmt.Fprintln(globalContext, "Failed to connect to cluster, please check your network connectivity to the specified address.")
			return err
		}
		return err
	}
	for idx, key := range resp.Keys {
		if key == "zookeeper_root" {
			fmt.Fprintln(globalContext, resp.Values[idx])
		}
	}

	return nil
}

func requireUseTable(run func(*grumble.Context) error) func(c *grumble.Context) error {
	grumbleRun := func(c *grumble.Context) error {
		if globalContext.UseTable == nil {
			c.App.PrintError(errors.New("please USE a table first"))
			c.App.Println("Usage: USE <TABLE_NAME>")
			return nil
		}
		return run(c)
	}
	return grumbleRun
}

// filterStringWithPrefix returns strings with the same prefix.
// This function is commonly used for the auto-completion of commands.
func filterStringWithPrefix(strs []string, prefix string) []string {
	var result []string
	for _, s := range strs {
		if strings.HasPrefix(s, prefix) {
			result = append(result, s)
		}
	}
	return result
}
