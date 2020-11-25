package executor

import (
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/olekukonko/tablewriter"
)

// ServerInfo command
func ServerInfo(client *Client) error {

	var results []*commandResult
	respMetas, errMeta := sendRemoteCommand(client, session.NodeTypeMeta, client.MetaAddresses, "server-info", []string{""})
	if errMeta != nil {
		return errMeta
	}
	respReplicas, errReplica := sendRemoteCommand(client, session.NodeTypeReplica, client.ReplicaAddresses, "server-info", []string{""})
	if errReplica != nil {
		return errReplica
	}
	results = append(results, respMetas...)
	results = append(results, respReplicas...)

	tabular := tablewriter.NewWriter(client)
	tabular.SetAutoFormatHeaders(false)
	tabular.SetAlignment(tablewriter.ALIGN_CENTER)
	tabular.SetColWidth(150)
	tabular.SetHeader([]string{"Server", "Node", "Version"})

	for _, result := range results {
		tabular.Append([]string{string(result.NodeType), result.Address, result.Result})
	}
	tabular.Render()

	return nil
}
