package executor

import (
	"admin-cli/tabular"
	"context"
	"strings"
	"time"
)

// ShowTablePartitions is table-partitions command
func ShowTablePartitions(client *Client, tableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, err := client.Meta.QueryConfig(ctx, tableName)
	if err != nil {
		return err
	}

	type partitionStruct struct {
		Pidx            int32  `json:"pidx"`
		PrimaryAddr     string `json:"primary"`
		SecondariesAddr string `json:"secondaries"`
	}

	var partitions []interface{}
	for _, partition := range resp.Partitions {
		p := partitionStruct{}
		p.Pidx = partition.Pid.PartitionIndex

		primary := client.Nodes.MustGetReplica(partition.Primary.GetAddress())
		p.PrimaryAddr = primary.CombinedAddr()

		var secondaries []string
		for _, sec := range partition.Secondaries {
			secNode := client.Nodes.MustGetReplica(sec.GetAddress())
			secondaries = append(secondaries, secNode.CombinedAddr())
		}
		p.SecondariesAddr = strings.Join(secondaries, ",")

		partitions = append(partitions, p)
	}

	tabular.Print(client, partitions)
	return nil
}
