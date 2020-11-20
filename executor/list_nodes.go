package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
)

// TODO(jiashuo1) support query detail info
// ListNodes command.
func ListNodes(client *Client, useJSON bool, enableResolve bool, file string) error {
	if len(file) != 0 {
		Save2File(client, file)
	} else {
		client.Writer = os.Stdout
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.Meta.ListNodes(ctx, &admin.ListNodesRequest{
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
		var addr = ""
		if enableResolve {
			addr, err = Resolve(node.Address.GetAddress(), Addr2Host)
			if err != nil {
				return err
			}
		} else {
			addr = node.Address.GetAddress()
		}

		nodeInfos = append(nodeInfos, nodeStruct{
			Node:   addr,
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
	for _, nodeInfo := range nodeInfos {
		tabular.Append([]string{nodeInfo.Node, nodeInfo.Status})
	}

	tabular.Render()
	return nil
}
