package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
)

type NodeDetailInfo struct {
	// nodes
	Node   string
	Status string
	// nodes -d
	ReplicaCount   int
	PrimaryCount   int
	SecondaryCount int
	// nodes -u
	MemUsedMB          int64
	BlockCacheMB       int64
	MemTableMB         int64
	MemIdxMB           int64
	DiskTotalMB        int64
	DiskAvailableMb    int64
	DiskAvailableRatio int64
	// nodes -q
	GetQPS    int64
	MgetQPS   int64
	PutQPS    int64
	MputQPS   int64
	GetBytes  int64
	MgetBytes int64
	PutBytes  int64
	MputBytes int64
	GetP99    int64
	MgetP99   int64
	PutP99    int64
	MputP99   int64
}

func (node *NodeDetailInfo) setStatus(info *admin.NodeInfo, enableResolve bool) error {
	if enableResolve {
		addr, err := Resolve(info.Address.GetAddress(), Addr2Host)
		if err != nil {
			return err
		}
		node.Node = addr
	} else {
		node.Node = info.Address.GetAddress()
	}

	node.Status = info.Status.String()
	return nil
}

func (node *NodeDetailInfo) setReplicaCount(client *Client, addr string, apps []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for _, app := range apps {
		partitionInfoResp, err := client.Meta.QueryConfig(ctx, app)
		if err != nil {
			return err
		}

		for _, partition := range partitionInfoResp.Partitions {
			if partition.Primary.GetAddress() == addr {
				node.PrimaryCount++
			}

			for _, secondary := range partition.Secondaries {
				if secondary.GetAddress() == addr {
					node.SecondaryCount++
				}
			}
		}
	}

	node.ReplicaCount = node.PrimaryCount + node.SecondaryCount
	return nil
}

func (node *NodeDetailInfo) setUsageInfo(client *Client, addr string, apps []string) error {
	//TODO(jiashuo1)
	return nil
}

func (node *NodeDetailInfo) setQPSInfo(client *Client, addr string, apps []string) error {
	//TODO(jiashuo1)
	return nil
}

func queryNodeDetailInfo(client *Client, app string, enableResolve bool) ([]*NodeDetailInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	listNodeResp, err := client.Meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	})
	if err != nil {
		return nil, err
	}

	var apps []string
	if len(app) == 0 {
		listAppsResp, err := client.Meta.ListApps(ctx, &admin.ListAppsRequest{
			Status: admin.AppStatus_AS_AVAILABLE,
		})
		if err != nil {
			return nil, err
		}
		for _, app := range listAppsResp.Infos {
			apps = append(apps, app.AppName)
		}
	} else {
		apps = append(apps, app)
	}

	var nodes []*NodeDetailInfo
	for _, info := range listNodeResp.Infos {
		var node = NodeDetailInfo{}
		_ = node.setStatus(info, enableResolve)
		_ = node.setUsageInfo(client, info.Address.GetAddress(), apps)
		_ = node.setReplicaCount(client, info.Address.GetAddress(), apps)
		_ = node.setQPSInfo(client, info.Address.GetAddress(), apps)
		nodes = append(nodes, &node)
	}

	return nodes, nil
}

// TODO(jiashuo1) support query detail info
func ListNodes(client *Client, app string, detail bool, usage bool, qps bool, useJSON bool, enableResolve bool) error {

	nodes, err := queryNodeDetailInfo(client, app, enableResolve)
	if err != nil {
		return err
	}

	if useJSON {
		// formats into JSON
		outputBytes, err := json.MarshalIndent(nodes, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(client, string(outputBytes))
		return nil
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(client)
	tabular.SetAlignment(tablewriter.ALIGN_CENTER)
	if detail {
		tabular.SetHeader([]string{"Node", "Status", "ReplicaCount", "PrimaryCount", "SecondaryCount"})
	} else if usage {
		tabular.SetHeader([]string{
			"Node", "Status", "MemUsedMB", "BlockCacheMB", "MemTableMB", "MemIdxMB",
			"DiskTotalMB", "DiskAvailableMb", "DiskAvailableRatio"})
	} else if qps {
		tabular.SetHeader([]string{
			"Node", "Status",
			"GetQPS", "MgetQPS", "PutQPS", "MputQPS",
			"GetBytes", "MgetBytes", "PutBytes", "MputBytes",
			"GetP99", "MgetP99", "PutP99", "MputP99"})
	} else {
		tabular.SetHeader([]string{"Node", "Status"})
	}

	for _, node := range nodes {
		if detail {
			tabular.Append([]string{
				node.Node, node.Status, strconv.Itoa(node.ReplicaCount),
				strconv.Itoa(node.PrimaryCount), strconv.Itoa(node.SecondaryCount)})
		} else if usage {
			tabular.Append([]string{})
		} else if qps {
			tabular.Append([]string{})
		} else {
			tabular.Append([]string{node.Node, node.Status})
		}

	}

	tabular.Render()
	return nil
}
