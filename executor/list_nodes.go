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

type NodeDetailInfo struct {
	// nodes
	Node           string
	Status         string
	ReplicaCount   int
	PrimaryCount   int
	SecondaryCount int
	// nodes -d
	DiskTotalMB  int64
	DiskUsedMb   int64
	DiskRatio    int64
	MemUsedMB    int64
	BlockCacheMB int64
	MemTableMB   int64
	MemIdxMB     int64
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

func ListNodes(client *Client, detail bool) error {

	nodes, err := queryNodeDetailInfo(client)
	if err != nil {
		return err
	}

	var rowDatas [][]string
	for _, node := range nodes {
		rowDatas = append(rowDatas, []string{
			node.Node,
			node.Status,
			strconv.Itoa(node.ReplicaCount),
			strconv.Itoa(node.PrimaryCount),
			strconv.Itoa(node.SecondaryCount),
			strconv.FormatInt(node.DiskTotalMB, 10),
			strconv.FormatInt(node.DiskUsedMb, 10),
			strconv.FormatInt(node.DiskRatio, 10),
			strconv.FormatInt(node.MemUsedMB, 10),
			strconv.FormatInt(node.BlockCacheMB, 10),
			strconv.FormatInt(node.MemTableMB, 10),
			strconv.FormatInt(node.MemIdxMB, 10),
			strconv.FormatInt(node.GetQPS, 10),
			strconv.FormatInt(node.MgetQPS, 10),
			strconv.FormatInt(node.PutQPS, 10),
			strconv.FormatInt(node.MputQPS, 10),
			strconv.FormatInt(node.GetBytes, 10),
			strconv.FormatInt(node.MgetBytes, 10),
			strconv.FormatInt(node.PutBytes, 10),
			strconv.FormatInt(node.MputBytes, 10)})
	}

	var baseHeader = []string{"Node", "Status"}
	var replicaCountHeader = []string{"Replica", "Primary", "Secondary"}
	var usageHeader = []string{"DiskTotalMB", "DiskUsedMb", "DiskRatio", "MemUsedMB", "BlockCacheMB", "MemTableMB", "MemIdxMB"}
	var requestHeader = []string{"GetQPS", "MgetQPS", "PutQPS", "MputQPS", "GetBytes", "MgetBytes", "PutBytes", "MputBytes"}
	var headers = [][]string{replicaCountHeader, usageHeader, requestHeader}

	var headerIndex = 2
	for _, header := range headers {
		tabular := tablewriter.NewWriter(client)
		tabular.SetAlignment(tablewriter.ALIGN_CENTER)
		tabular.SetHeader(append(baseHeader, header...))
		tabular.SetAutoFormatHeaders(false)

		for _, rowData := range rowDatas {
			tabular.Append(append([]string{rowData[0], rowData[1]}, rowData[headerIndex:headerIndex+len(header)]...))
		}
		headerIndex += len(header)

		if !detail {
			tabular.Render()
			return nil
		}

		tabular.Render()
	}

	return nil
}

func queryNodeDetailInfo(client *Client) ([]*NodeDetailInfo, error) {
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
		statusErr := node.setStatus(info)
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

func (node *NodeDetailInfo) setStatus(info *admin.NodeInfo) error {
	host, err := helper.Resolve(info.Address.GetAddress(), helper.Addr2Host)
	if err != nil {
		return err
	}
	node.Node = fmt.Sprintf("%s[%s]", host, info.Address.GetAddress())

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
	node.DiskUsedMb = node.DiskTotalMB - helper.GetNodeCounterValue(perfClient, "disk.available.total(MB)")
	node.DiskRatio = 100 - helper.GetNodeCounterValue(perfClient, "disk.available.total.ratio")
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
