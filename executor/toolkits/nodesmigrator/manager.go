package nodesmigrator

import (
	"fmt"
	"math"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/pegasus-kv/admin-cli/executor"
)

func MigrateAllReplicaToNodes(client *executor.Client, period int, from []string, to []string) error {
	nodesMigrator, err := createNewMigrator(client, from, to)
	if err != nil {
		return err
	}
	tables, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
	if err != nil {
		return fmt.Errorf("list app failed: %s", err.Error())
	}

	var targetIndex = -1
	var totalRemainingReplica = math.MaxInt16
	for {
		if totalRemainingReplica <= 0 {
			fmt.Printf("INFO: completed for all the targets has migrate\n")
			return executor.ListNodes(client)
		}
		targetIndex++
		round, currentTargetNode := nodesMigrator.getCurrentTargetNode(targetIndex)
		currentTargetNode.downgradeAllReplicaToSecondary(client)
		fmt.Printf("\n\n********[%d]start migrate replicas to %s******\n", round, currentTargetNode.String())
		fmt.Printf("INFO: migrate out all primary from current node %s\n", currentTargetNode.String())

		totalRemainingReplica = 0
		for _, tb := range tables {
			remainingCount := nodesMigrator.run(client, tb.AppName, round, currentTargetNode)
			totalRemainingReplica = totalRemainingReplica + remainingCount
		}
		time.Sleep(10 * time.Second)
	}
}
