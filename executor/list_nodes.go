package executor

import (
	"admin-cli/tabular"
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
)

type nodeInfoStruct struct {
	Address           string `json:"node"`
	Status            string `json:"status"`
	ReplicaTotalCount int    `json:"replica"`
	PrimaryCount      int    `json:"primary"`
	SecondaryCount    int    `json:"secondary"`
}

// ListNodes is nodes command.
func ListNodes(client *Client, table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	listNodeResp, err := client.Meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	})
	if err != nil {
		return err
	}

	nodes := make(map[string]*nodeInfoStruct)
	for _, ninfo := range listNodeResp.Infos {
		n := client.Nodes.MustGetReplica(ninfo.Address.GetAddress())
		nodes[ninfo.Address.GetAddress()] = &nodeInfoStruct{
			Address: n.CombinedAddr(),
			Status:  ninfo.Status.String(),
		}
	}
	nodes, err = fillNodesInfoMap(client, nodes)
	if err != nil {
		return err
	}

	// render in tabular form
	var nodeList []interface{}
	for _, n := range nodes {
		nodeList = append(nodeList, *n)
	}

	tbWriter := tabular.New(client, nodeList)
	footerWithTotalCount(tbWriter, nodeList)
	tbWriter.Render()
	return nil
}

func footerWithTotalCount(tbWriter *tablewriter.Table, nlist []interface{}) {
	var totalRepCnt, totalPriCnt, totalSecCnt int
	for _, element := range nlist {
		n := element.(nodeInfoStruct)
		totalRepCnt += n.ReplicaTotalCount
		totalPriCnt += n.PrimaryCount
		totalSecCnt += n.SecondaryCount
	}
	tbWriter.SetFooter([]string{
		"",
		"Total",
		fmt.Sprintf("%d", totalRepCnt),
		fmt.Sprintf("%d", totalPriCnt),
		fmt.Sprintf("%d", totalSecCnt),
	})
}

func fillNodesInfoMap(client *Client, nodes map[string]*nodeInfoStruct) (map[string]*nodeInfoStruct, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	listAppsResp, err := client.Meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return nil, err
	}
	var tableNames []string
	for _, info := range listAppsResp.Infos {
		tableNames = append(tableNames, info.AppName)
	}

	for _, tb := range tableNames {
		// reuse the previous context, failed if the total time expires
		queryCfgResp, err := client.Meta.QueryConfig(ctx, tb)
		if err != nil {
			return nil, err
		}
		for _, part := range queryCfgResp.Partitions {
			n := nodes[part.Primary.GetAddress()]
			if n == nil {
				return nil, fmt.Errorf("inconsistent state: nodes are updated")
			}
			n.PrimaryCount++
			n.ReplicaTotalCount++

			for _, sec := range part.Secondaries {
				n := nodes[sec.GetAddress()]
				n.SecondaryCount++
				n.ReplicaTotalCount++
			}
		}
	}

	return nodes, nil
}
