package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	if len(file) != 0 {
		save2File(client, file)
	} else {
		client.Writer = os.Stdout
	}

	if enableResolve {
		node, err := resolve(replicaServer, Host2Addr)
		if err != nil {
			return err
		}
		replicaServer = node
	}

	err := validateNodeAddress(client, replicaServer)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.replicaPool.GetReplica(replicaServer).QueryDiskInfo(ctx, &radmin.QueryDiskInfoRequest{
		Node:    &base.RPCAddress{}, //TODO(jiashuo1) this thrift variable is useless, it need be deleted on client/server
		AppName: tableName,
	})
	if err != nil {
		return err
	}

	switch infoType {
	case CapacitySize:
		queryDiskCapacity(client, resp, diskTag, useJSON, enableResolve)
		break
	case ReplicaCount:
		queryDiskReplicaCount(client, resp, useJSON, enableResolve)
		break
	default:
		break
	}
	return nil
}

func queryDiskCapacity(client *Client, resp *radmin.QueryDiskInfoResponse, diskTag string, useJSON bool, enableResolve bool) {

	type NodeCapacityStruct struct {
		Disk      string
		Capacity  int64
		Available int64
		Ratio     int64
	}

	type ReplicaCapacityStruct struct {
		Replica  string
		Status   string
		Capacity string
	}

	var nodeCapacityInfos []NodeCapacityStruct
	var replicaCapacityInfos []ReplicaCapacityStruct

	for _, diskInfo := range resp.DiskInfos {
		// pass disk tag means query one disk detail capacity of replica
		if len(diskTag) != 0 && diskInfo.Tag == diskTag {
			appendReplicaCapacityInfo := func(replicasWithAppId map[int32][]*base.Gpid, replicaStatus string) {
				for _, replicas := range replicasWithAppId {
					for _, replica := range replicas {
						replicaCapacityInfos = append(replicaCapacityInfos, ReplicaCapacityStruct{
							Replica:  fmt.Sprintf("%d.%d", replica.Appid, replica.PartitionIndex),
							Status:   replicaStatus,
							Capacity: "TODO(jiashuo1)", // TODO(jiashuo1) need send remote command to get the replica storage
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
				return
			}

			// formats into tabular
			tabular := tablewriter.NewWriter(client)
			tabular.SetHeader([]string{"Replica", "Status", "Capacity"})
			tabular.SetAlignment(tablewriter.ALIGN_CENTER)
			for _, replicaCapacityInfo := range replicaCapacityInfos {
				tabular.Append([]string{
					replicaCapacityInfo.Replica,
					replicaCapacityInfo.Status,
					replicaCapacityInfo.Capacity})
			}
			tabular.Render()
			return
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
		return
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
	return
}

func queryDiskReplicaCount(client *Client, resp *radmin.QueryDiskInfoResponse, useJSON bool, enableResolve bool) {
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
