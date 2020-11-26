package executor

import (
	"admin-cli/helper"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
)

type NodeInfo struct {
	NodeAddress       string
	NodeStatus        string
	ReplicaTotalCount int
	PrimaryCount      int
	SecondaryCount    int
}

func ListNodes(client *Client, app string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	listNodeResp, err := client.Meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	})
	if err != nil {
		return err
	}

	var apps []string
	if len(app) == 0 {
		listAppsResp, err := client.Meta.ListApps(ctx, &admin.ListAppsRequest{
			Status: admin.AppStatus_AS_AVAILABLE,
		})
		if err != nil {
			return err
		}
		for _, app := range listAppsResp.Infos {
			apps = append(apps, app.AppName)
		}
	}
	apps = append(apps, app)

	var nodeInfos []*NodeInfo
	for _, info := range listNodeResp.Infos {
		nodeInfo := NodeInfo{}
		addr := info.Address.GetAddress()
		host, err := helper.Resolve(addr, helper.Addr2Host)
		if err != nil {
			return err
		}
		nodeInfo.NodeAddress = fmt.Sprintf("%s[%s]", host, addr)
		nodeInfo.NodeStatus = info.Status.String()

		for _, app := range apps {
			partitionInfoResp, err := client.Meta.QueryConfig(ctx, app)
			if err != nil {
				return err
			}

			for _, partition := range partitionInfoResp.Partitions {
				if partition.Primary.GetAddress() == addr {
					nodeInfo.PrimaryCount++
				}

				for _, secondary := range partition.Secondaries {
					if secondary.GetAddress() == addr {
						nodeInfo.SecondaryCount++
					}
				}
			}
		}
		nodeInfo.ReplicaTotalCount = nodeInfo.PrimaryCount + nodeInfo.SecondaryCount
		nodeInfos = append(nodeInfos, &nodeInfo)
	}

	tabular := tablewriter.NewWriter(client)
	tabular.SetAlignment(tablewriter.ALIGN_CENTER)
	tabular.SetHeader([]string{"Node", "Status", "Replica", "Primary", "Secondary"})
	tabular.SetAutoFormatHeaders(false)
	var totalReplica = 0
	var totalPrimary = 0
	var totalSecondary = 0
	for _, info := range nodeInfos {
		totalReplica += info.ReplicaTotalCount
		totalPrimary += info.PrimaryCount
		totalSecondary += info.SecondaryCount
		tabular.Append([]string{info.NodeAddress, info.NodeStatus, strconv.Itoa(info.ReplicaTotalCount), strconv.Itoa(info.PrimaryCount), strconv.Itoa(info.SecondaryCount)})
	}
	tabular.Append([]string{"", "", strconv.Itoa(totalReplica), strconv.Itoa(totalPrimary), strconv.Itoa(totalSecondary)})
	tabular.Render()
	return nil
}
