package executor

import (
	"admin-cli/helper"
	"context"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
	"strconv"
	"time"
)

type nodeStatStruct struct {
	Node      string
	Status    string
	GetQPS    int64
	MgetQPS   int64
	PutQPS    int64
	MputQPS   int64
	GetBytes  int64
	MgetBytes int64
	PutBytes  int64
	MputBytes int64
	// nodes-stat -d
	DiskTotalMB  int64
	DiskUsedMb   int64
	DiskRatio    int64
	MemUsedMB    int64
	BlockCacheMB int64
	MemTableMB   int64
	MemIdxMB     int64
}

func ShowNodesStat(client *Client, detail bool) error {
	nodeStats, err := queryNodesStat(client)
	if err != nil {
		return err
	}
	var rowDatas [][]string
	for _, stat := range nodeStats {
		rowDatas = append(rowDatas, []string{
			stat.Node,
			stat.Status,
			strconv.FormatInt(stat.GetQPS, 10),
			strconv.FormatInt(stat.MgetQPS, 10),
			strconv.FormatInt(stat.PutQPS, 10),
			strconv.FormatInt(stat.MputQPS, 10),
			strconv.FormatInt(stat.GetBytes, 10),
			strconv.FormatInt(stat.MgetBytes, 10),
			strconv.FormatInt(stat.PutBytes, 10),
			strconv.FormatInt(stat.MputBytes, 10),
			strconv.FormatInt(stat.DiskTotalMB, 10),
			strconv.FormatInt(stat.DiskUsedMb, 10),
			strconv.FormatInt(stat.DiskRatio, 10) + "%",
			strconv.FormatInt(stat.MemUsedMB, 10),
			strconv.FormatInt(stat.BlockCacheMB, 10),
			strconv.FormatInt(stat.MemTableMB, 10),
			strconv.FormatInt(stat.MemIdxMB, 10)})
	}

	var baseHeader = []string{"Node", "Status"}
	var requestHeader = []string{"GetQPS", "MgetQPS", "PutQPS", "MputQPS", "GetBytes", "MgetBytes", "PutBytes", "MputBytes"}
	var usageHeader = []string{"DiskMB", "DiskUsedMB", "DiskRatio", "MemUsedMB", "BlockCacheMB", "MemTableMB", "MemIdxMB"}
	var headers = [][]string{requestHeader, usageHeader}

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
			fmt.Println("[Node Request Stats]")
			tabular.Render()
			return nil
		}
		fmt.Println("[Node Usage Stats]")
		tabular.Render()
	}
	return nil
}

func queryNodesStat(client *Client) ([]*nodeStatStruct, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	listNodeResp, err := client.Meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	})
	if err != nil {
		return nil, err
	}

	var nodes []*nodeStatStruct
	for _, info := range listNodeResp.Infos {
		var node = nodeStatStruct{}
		statusErr := node.setStatus(info)
		if statusErr != nil {
			return nil, statusErr
		}
		usageErr := node.setUsageInfo(client, info.Address.GetAddress())
		if usageErr != nil {
			return nil, usageErr
		}
		QPSErr := node.setQPSInfo(client, info.Address.GetAddress())
		if QPSErr != nil {
			return nil, QPSErr
		}
		nodes = append(nodes, &node)
	}

	return nodes, nil
}

func (node *nodeStatStruct) setStatus(info *admin.NodeInfo) error {
	host, err := helper.Resolve(info.Address.GetAddress(), helper.Addr2Host)
	if err != nil {
		return err
	}
	node.Node = fmt.Sprintf("%s[%s]", host, info.Address.GetAddress())

	node.Status = info.Status.String()
	return nil
}

func (node *nodeStatStruct) setUsageInfo(client *Client, addr string) error {
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

func (node *nodeStatStruct) setQPSInfo(client *Client, addr string) error {
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
