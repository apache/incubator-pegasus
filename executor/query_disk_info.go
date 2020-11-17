package executor

import (
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

// ListNodes command.
func QueryDiskInfo(client *Client, infoType DiskInfoType, replicaServer string, tableName string, diskTag string, useJSON bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.replicaPool.GetReplica(replicaServer).QueryDiskInfo(ctx, &radmin.QueryDiskInfoRequest{
		Node:    &base.RPCAddress{},
		AppName: tableName,
	})
	if err != nil {
		return err
	}

	switch infoType {
	case CapacitySize:
		queryDiskCapacity(client, resp, diskTag, useJSON)
		break
	case ReplicaCount:
		queryDiskReplicaCount(client, resp, useJSON)
	}
	return nil
}

func queryDiskCapacity(client *Client, resp *radmin.QueryDiskInfoResponse, diskTag string, useJSON bool) {

	type NodeCapacityStruct struct {
		Disk      string `json:"disk tag"`
		Capacity  int64  `json:"disk total capacity"`
		Available int64  `json:"disk available capacity"`
		Ratio     int64  `json:"disk available capacity ratio"`
	}

	type ReplicaCapacityStruct struct {
		Replica  string `json:"replica gpid"`
		Status   string `json:"replica status"`
		Capacity string `json:"replica capacity"`
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
							Replica:  replica.String(),
							Status:   replicaStatus,
							Capacity: "TODO(jiashuo1)", // TODO(jiashuo1) need send other query to get the replica capacity
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

func queryDiskReplicaCount(client *Client, resp *radmin.QueryDiskInfoResponse, useJSON bool) {
	type ReplicaCountStruct struct {
		Disk      string `json:"disk tag"`
		Primary   int    `json:"disk primary replica count"`
		Secondary int    `json:"disk secondary replica count"`
		Total     int    `json:"total replica count"`
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
	for _, replicaCountInfo := range replicaCountInfos {
		tabular.Append([]string{
			replicaCountInfo.Disk,
			string(rune(replicaCountInfo.Primary)),
			string(rune(replicaCountInfo.Secondary)),
			string(rune(replicaCountInfo.Total))})
	}
	tabular.Render()
	return
}
