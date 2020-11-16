package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
)

// ClusterInfo command
func ClusterInfo(client *Client, useJSON bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.meta.QueryClusterInfo(ctx, &admin.ClusterInfoRequest{})
	if err != nil {
		return err
	}

	var clusterInfoMap map[string]string
	for i, key := range resp.Keys {
		clusterInfoMap[key] = resp.Values[i]
	}

	if useJSON {
		// formats into JSON
		outputBytes, err := json.MarshalIndent(clusterInfoMap, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(client, string(outputBytes))
		return nil
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(client)
	for key, value := range clusterInfoMap {
		tabular.Append([]string{key, value})
	}
	tabular.Render()
	return nil
}
