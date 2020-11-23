package executor

import (
	"admin-cli/helper"
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
	DiskTotalMB        int64
	DiskAvailableMb    int64
	DiskAvailableRatio int64
	MemUsedMB          int64
	BlockCacheMB       int64
	MemTableMB         int64
	MemIdxMB           int64
	// nodes -q
	GetQPS    int64
	MgetQPS   int64
	PutQPS    int64
	MputQPS   int64
	GetBytes  int64
	MgetBytes int64
	PutBytes  int64
	MputBytes int64
}

func ListNodes(client *Client, detail bool, usage bool, qps bool, useJSON bool, enableResolve bool) error {

	nodes, err := queryNodeDetailInfo(client, enableResolve)
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
			"Node", "Status", "DiskTotalMB", "DiskAvailableMb", "DiskAvailableRatio",
			"MemUsedMB", "BlockCacheMB", "MemTableMB", "MemIdxMB"})
	} else if qps {
		tabular.SetHeader([]string{
			"Node", "Status",
			"GetQPS", "MgetQPS", "PutQPS", "MputQPS",
			"GetBytes", "MgetBytes", "PutBytes", "MputBytes"})
	} else {
		tabular.SetHeader([]string{"Node", "Status"})
	}

	for _, node := range nodes {
		if detail {
			tabular.Append([]string{
				node.Node, node.Status, strconv.Itoa(node.ReplicaCount),
				strconv.Itoa(node.PrimaryCount), strconv.Itoa(node.SecondaryCount)})
		} else if usage {
			// nodes -u
			tabular.Append([]string{
				node.Node, node.Status,
				strconv.FormatInt(node.DiskTotalMB, 10),
				strconv.FormatInt(node.DiskAvailableMb, 10),
				strconv.FormatInt(node.DiskAvailableRatio, 10),
				strconv.FormatInt(node.MemUsedMB, 10),
				strconv.FormatInt(node.BlockCacheMB, 10),
				strconv.FormatInt(node.MemTableMB, 10),
				strconv.FormatInt(node.MemIdxMB, 10)})
		} else if qps {
			tabular.Append([]string{
				node.Node, node.Status,
				strconv.FormatInt(node.GetQPS, 10),
				strconv.FormatInt(node.MgetQPS, 10),
				strconv.FormatInt(node.PutQPS, 10),
				strconv.FormatInt(node.MputQPS, 10),
				strconv.FormatInt(node.GetBytes, 10),
				strconv.FormatInt(node.MgetBytes, 10),
				strconv.FormatInt(node.PutBytes, 10),
				strconv.FormatInt(node.MputBytes, 10)})
		} else {
			tabular.Append([]string{node.Node, node.Status})
		}
	}

	tabular.Render()
	return nil
}

func queryNodeDetailInfo(client *Client, enableResolve bool) ([]*NodeDetailInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	listNodeResp, err := client.Meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	})
	if err != nil {
		return nil, err
	}

	var nodes []*NodeDetailInfo
	for _, info := range listNodeResp.Infos {
		var node = NodeDetailInfo{}
		statusErr := node.setStatus(info, enableResolve)
		if statusErr != nil {
			return nil, statusErr
		}
		usageErr := node.setUsageInfo(client, info.Address.GetAddress())
		if usageErr != nil {
			return nil, usageErr
		}
		replicaCountErr := node.setReplicaCount(client, info.Address.GetAddress())
		if replicaCountErr != nil {
			return nil, replicaCountErr
		}
		QPSErr := node.setQPSInfo(client, info.Address.GetAddress())
		if QPSErr != nil {
			return nil, QPSErr
		}
		nodes = append(nodes, &node)
	}

	return nodes, nil
}

func (node *NodeDetailInfo) setStatus(info *admin.NodeInfo, enableResolve bool) error {
	if enableResolve {
		addr, err := helper.Resolve(info.Address.GetAddress(), helper.Addr2Host)
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

func (node *NodeDetailInfo) setReplicaCount(client *Client, addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var apps []string
	listAppsResp, err := client.Meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return err
	}
	for _, app := range listAppsResp.Infos {
		apps = append(apps, app.AppName)
	}

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

func (node *NodeDetailInfo) setUsageInfo(client *Client, addr string) error {
	perfClient, err := client.GetPerfCounterClient(addr)
	if err != nil {
		return err
	}
	node.DiskTotalMB = helper.GetNodeCounterValue(perfClient, "disk.capacity.total")
	node.DiskAvailableMb = helper.GetNodeCounterValue(perfClient, "disk.available.total(MB)")
	node.DiskAvailableRatio = helper.GetNodeCounterValue(perfClient, "disk.available.total.ratio")
	node.MemUsedMB = helper.GetNodeCounterValue(perfClient, "memused.res")
	node.BlockCacheMB = helper.GetNodeCounterValue(perfClient, "rdb.block_cache.memory_usage")
	node.MemTableMB = helper.GetNodeAggregateCounterValue(perfClient, "rdb.memtable.memory_usage") >> 20
	node.MemIdxMB = helper.GetNodeAggregateCounterValue(perfClient, "rdb.index_and_filter_blocks.memory_usage") >> 20
	return nil
}

func (node *NodeDetailInfo) setQPSInfo(client *Client, addr string) error {
	perfClient, err := client.GetPerfCounterClient(addr)
	if err != nil {
		return err
	}

	node.GetQPS = helper.GetNodeAggregateCounterValue(perfClient, "get_qps")
	node.PutQPS = helper.GetNodeAggregateCounterValue(perfClient, "put_qps")
	node.MgetQPS = helper.GetNodeAggregateCounterValue(perfClient, "multi_get_qps")
	node.MputQPS = helper.GetNodeAggregateCounterValue(perfClient, "multi_put_qps")

	node.GetBytes = helper.GetNodeAggregateCounterValue(perfClient, "get_bytes")
	node.PutBytes = helper.GetNodeAggregateCounterValue(perfClient, "put_bytes")
	node.MgetBytes = helper.GetNodeAggregateCounterValue(perfClient, "multi_get_bytes")
	node.MputBytes = helper.GetNodeAggregateCounterValue(perfClient, "multi_put_bytes")

	return nil
}
