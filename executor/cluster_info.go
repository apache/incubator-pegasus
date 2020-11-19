package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

// ClusterInfo command
func ClusterInfo(client *Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.meta.QueryClusterInfo(ctx, &admin.ClusterInfoRequest{})
	if err != nil {
		return err
	}

	clusterInfoMap := make(map[string]string)
	for i, key := range resp.Keys {
		clusterInfoMap[key] = resp.Values[i]
	}

	// formats into JSON
	outputBytes, err := json.MarshalIndent(clusterInfoMap, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(client, string(outputBytes))
	return nil
}
