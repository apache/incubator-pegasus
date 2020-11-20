package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/olekukonko/tablewriter"
)

func RemoteCommand(client *Client, targetType string, addr string, cmd string, args []string, file string, enableResolve bool) error {
	if enableResolve {
		var node, err = Resolve(addr, Host2Addr)
		if err != nil {
			return err
		}
		addr = node
	}

	var nodeType session.NodeType
	// TODO(jiashuo1) shell command support `option` args
	if targetType == "meta" {
		nodeType = session.NodeTypeReplica
	} else if targetType == "replica" {
		nodeType = session.NodeTypeReplica
	} else {
		return fmt.Errorf("Please input `meta` or `replica`")
	}

	resp, err := SendRemoteCommand(client, nodeType, addr, cmd, args)
	if err != nil {
		return err
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(client)
	tabular.SetHeader([]string{"node", "result"})
	tabular.Append([]string{addr, resp})
	tabular.Render()
	return nil
}

func SendRemoteCommand(client *Client, nodeType session.NodeType, addr string, cmd string, args []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, err := client.GetRemoteCommandClient(addr, nodeType).Call(ctx, cmd, args)
	if err != nil {
		return "", err
	} else {
		return resp, nil
	}

}
