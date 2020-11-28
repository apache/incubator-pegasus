package executor

import (
	"github.com/XiaoMi/pegasus-go-client/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/olekukonko/tablewriter"
)

// ServerInfo command
func ServerInfo(client *Client) error {

	nodes := client.Nodes.GetAllNodes(session.NodeTypeMeta)
	nodes = append(nodes, client.Nodes.GetAllNodes(session.NodeTypeReplica)...)

	results := batchCallCmd(nodes, &admin.RemoteCommand{
		Command:   "server-info",
		Arguments: []string{},
	})

	tabular := tablewriter.NewWriter(client)
	tabular.SetAutoFormatHeaders(false)
	tabular.SetAlignment(tablewriter.ALIGN_CENTER)
	tabular.SetColWidth(150)
	tabular.SetHeader([]string{"Server", "Node", "Version"})

	for n, result := range results {
		tabular.Append([]string{string(n.Type), n.CombinedAddr(), result.String()})
	}
	tabular.Render()

	return nil
}
