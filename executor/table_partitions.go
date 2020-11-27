package executor

import (
	"admin-cli/helper"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/olekukonko/tablewriter"
)

func ShowTablePartitions(client *Client, tableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, err := client.Meta.QueryConfig(ctx, tableName)
	if err != nil {
		return err
	}

	type partitionConfigurationStruct struct {
		Pidx               int32
		PrimaryAddress     string
		SecondariesAddress string
	}
	var partitionConfigurations []*partitionConfigurationStruct
	for _, partition := range resp.Partitions {
		partitionConfiguration := partitionConfigurationStruct{}
		partitionConfiguration.Pidx = partition.Pid.PartitionIndex

		host, err := helper.Resolve(partition.Primary.GetAddress(), helper.Addr2Host)
		if err != nil {
			return err
		}
		primary := fmt.Sprintf("%s[%s]", host, partition.Primary.GetAddress())
		partitionConfiguration.PrimaryAddress = primary

		var secondaries = ""
		for _, secondary := range partition.Secondaries {
			host, err := helper.Resolve(secondary.GetAddress(), helper.Addr2Host)
			if err != nil {
				return err
			}
			secondaries = fmt.Sprintf("%s[%s(%s)]", secondaries, host, secondary.GetAddress())
		}
		partitionConfiguration.SecondariesAddress = secondaries

		partitionConfigurations = append(partitionConfigurations, &partitionConfiguration)
	}

	tabular := tablewriter.NewWriter(client)
	tabular.SetAlignment(tablewriter.ALIGN_CENTER)
	tabular.SetHeader([]string{"Pidx", "Primary", "Secondaries"})
	tabular.SetAutoFormatHeaders(false)
	for _, info := range partitionConfigurations {
		tabular.Append([]string{strconv.Itoa(int(info.Pidx)), info.PrimaryAddress, info.SecondariesAddress})
	}
	fmt.Printf("[ShowTablePartitions(ReplicaCount=%d)]\n", resp.Partitions[0].MaxReplicaCount)
	tabular.Render()
	return nil
}
