package executor

import (
	"admin-cli/helper"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/radmin"
	"github.com/olekukonko/tablewriter"
)

type DiskInfoType int32

const (
	CapacitySize DiskInfoType = 0
	ReplicaCount DiskInfoType = 1
)

// QueryDiskInfo command
func QueryDiskInfo(client *Client, infoType DiskInfoType, replicaServer string, tableName string, diskTag string, file string, useJSON bool, enableResolve bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if enableResolve {
		var node, err = helper.Resolve(replicaServer, helper.Host2Addr)
		if err != nil {
			return err
		}
		replicaServer = node
	}

	replicaClient, err := client.GetReplicaClient(replicaServer)
	if err != nil {
		return err
	}
	resp, err := replicaClient.QueryDiskInfo(ctx, &radmin.QueryDiskInfoRequest{
		Node:    &base.RPCAddress{}, //TODO(jiashuo1) this thrift variable is useless, it need be deleted on client/server
		AppName: tableName,
	})
	if err != nil {
		return err
	}

	switch infoType {
	case CapacitySize:
		queryDiskCapacity(client, replicaServer, resp, diskTag, useJSON)
		break
	case ReplicaCount:
		queryDiskReplicaCount(client, resp, useJSON)
		break
	default:
		break
	}
	return nil
}

func queryDiskCapacity(client *Client, replicaServer string, resp *radmin.QueryDiskInfoResponse, diskTag string, useJSON bool) error {

	type NodeCapacityStruct struct {
		Disk      string
		Capacity  int64
		Available int64
		Ratio     int64
	}

	type ReplicaCapacityStruct struct {
		Replica  string
		Status   string
		Capacity float64
	}

	var nodeCapacityInfos []NodeCapacityStruct
	var replicaCapacityInfos []ReplicaCapacityStruct

	perfClient, err := client.GetPerfCounterClient(replicaServer)
	if err != nil {
		return err
	}

	for _, diskInfo := range resp.DiskInfos {
		// pass disk tag means query one disk detail capacity of replica
		if len(diskTag) != 0 && diskInfo.Tag == diskTag {
			appendReplicaCapacityInfo := func(replicasWithAppId map[int32][]*base.Gpid, replicaStatus string) {
				for _, replicas := range replicasWithAppId {
					for _, replica := range replicas {
						var gpidStr = fmt.Sprintf("%d.%d", replica.Appid, replica.PartitionIndex)
						replicaCapacityInfos = append(replicaCapacityInfos, ReplicaCapacityStruct{
							Replica:  gpidStr,
							Status:   replicaStatus,
							Capacity: float64(helper.GetReplicaCounterValue(perfClient, "disk.storage.sst(MB)", gpidStr)),
						})
					}
				}
			}
			appendReplicaCapacityInfo(diskInfo.HoldingPrimaryReplicas, "primary")
			appendReplicaCapacityInfo(diskInfo.HoldingSecondaryReplicas, "secondary")

			if useJSON {
				// formats into JSON
				outputBytes, err := json.MarshalIndent(replicaCapacityInfos, "", "  ")
				if err != nil {
					fmt.Println(err)
				}
				fmt.Fprintln(client, string(outputBytes))
				return nil
			}

			// formats into tabular
			tabular := tablewriter.NewWriter(client)
			tabular.SetHeader([]string{"Replica", "Status", "Capacity"})
			tabular.SetAlignment(tablewriter.ALIGN_CENTER)
			for _, replicaCapacityInfo := range replicaCapacityInfos {
				tabular.Append([]string{
					replicaCapacityInfo.Replica,
					replicaCapacityInfo.Status,
					strconv.FormatFloat(replicaCapacityInfo.Capacity, 'f', -1, 64)})
			}
			tabular.Render()
			return nil
		}

		nodeCapacityInfos = append(nodeCapacityInfos, NodeCapacityStruct{
			Disk:      diskInfo.Tag,
			Capacity:  diskInfo.DiskCapacityMb,
			Available: diskInfo.DiskAvailableMb,
			Ratio:     diskInfo.DiskAvailableMb * 100.0 / diskInfo.DiskCapacityMb,
		})
	}

	if useJSON {
		// formats into JSON
		outputBytes, err := json.MarshalIndent(nodeCapacityInfos, "", "  ")
		if err != nil {
			fmt.Println(err)
		}
		fmt.Fprintln(client, string(outputBytes))
		return nil
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(client)
	tabular.SetAlignment(tablewriter.ALIGN_CENTER)
	tabular.SetHeader([]string{"Disk", "Capacity", "Available", "Ratio"})
	for _, nodeCapacityInfo := range nodeCapacityInfos {
		tabular.Append([]string{
			nodeCapacityInfo.Disk,
			strconv.FormatInt(nodeCapacityInfo.Capacity, 10),
			strconv.FormatInt(nodeCapacityInfo.Available, 10),
			strconv.FormatInt(nodeCapacityInfo.Ratio, 10)})
	}
	tabular.Render()
	return nil
}

func queryDiskReplicaCount(client *Client, resp *radmin.QueryDiskInfoResponse, useJSON bool) {
	type ReplicaCountStruct struct {
		Disk      string
		Primary   int
		Secondary int
		Total     int
	}

	computeReplicaCount := func(replicasWithAppId map[int32][]*base.Gpid) int {
		var replicaCount = 0
		for _, replicas := range replicasWithAppId {
			for _, _ = range replicas {
				replicaCount++
			}
		}
		return replicaCount
	}

	var replicaCountInfos []ReplicaCountStruct
	for _, diskInfo := range resp.DiskInfos {
		var primaryCount = computeReplicaCount(diskInfo.HoldingPrimaryReplicas)
		var secondaryCount = computeReplicaCount(diskInfo.HoldingSecondaryReplicas)
		replicaCountInfos = append(replicaCountInfos, ReplicaCountStruct{
			Disk:      diskInfo.Tag,
			Primary:   primaryCount,
			Secondary: secondaryCount,
			Total:     primaryCount + secondaryCount,
		})
	}

	if useJSON {
		// formats into JSON
		outputBytes, err := json.MarshalIndent(replicaCountInfos, "", "  ")
		if err != nil {
			fmt.Println(err)
		}
		fmt.Fprintln(client, string(outputBytes))
		return
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(client)
	tabular.SetHeader([]string{"Disk", "Primary", "Secondary", "Total"})
	tabular.SetAlignment(tablewriter.ALIGN_CENTER)
	for _, replicaCountInfo := range replicaCountInfos {
		tabular.Append([]string{
			replicaCountInfo.Disk,
			strconv.Itoa(replicaCountInfo.Primary),
			strconv.Itoa(replicaCountInfo.Secondary),
			strconv.Itoa(replicaCountInfo.Total)})
	}
	tabular.Render()
	return
}
