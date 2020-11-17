package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
)

// TODO(jiashuo1) support query detail info
// ListNodes command.
func ListNodes(client *Client, useJSON bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	})
	if err != nil {
		return err
	}

	type nodeStruct struct {
		Node   string `json:"node"`
		Status string `json:"status"`
	}
	var nodeInfos []nodeStruct
	for _, node := range resp.Infos {
		nodeInfos = append(nodeInfos, nodeStruct{
			Node:   node.Address.GetAddress(),
			Status: node.Status.String(),
		})
	}

	if useJSON {
		// formats into JSON
		outputBytes, err := json.MarshalIndent(nodeInfos, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(client, string(outputBytes))
		return nil
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(client)
	tabular.SetHeader([]string{"Node", "Status"})
	for _, nodeInfo := range resp.Infos {
		tabular.Append([]string{nodeInfo.Address.GetAddress(), nodeInfo.Status.String()})
	}
	tabular.Render()
	return nil
}
