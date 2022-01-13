package nodesmigrator

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/pegasus-kv/admin-cli/executor"
)

func MigrateAllReplicaToNodes(client *executor.Client, from []string, to []string, tables []string, batch bool, concurrent int) error {
	nodesMigrator, err := createNewMigrator(client, from, to)
	if err != nil {
		return err
	}

	var tableList []string
	if len(tables) != 0 && tables[0] != "" {
		tableList = tables
	} else {
		tbs, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
		if err != nil {
			return fmt.Errorf("list app failed: %s", err.Error())
		}
		for _, tb := range tbs {
			tableList = append(tableList, tb.AppName)
		}
	}

	currentOriginNode := nodesMigrator.selectNextOriginNode()
	firstOrigin := currentOriginNode
	var totalRemainingReplica int32 = math.MaxInt32
	round := -1
	for {
		if totalRemainingReplica <= 0 {
			logInfo("\n\n==============completed for all origin nodes has been migrated================")
			return executor.ListNodes(client)
		}

		if currentOriginNode.String() == firstOrigin.String() {
			round++
		}
		logInfo(fmt.Sprintf("\n\n*******************[%d|%s]start migrate replicas, remainingReplica=%d*****************",
			round, currentOriginNode.String(), totalRemainingReplica))
		currentOriginNode.downgradeAllReplicaToSecondary(client)

		totalRemainingReplica = 0
		tableCount := len(tableList)
		var wg sync.WaitGroup
		wg.Add(tableCount)
		for _, tb := range tableList {
			targetTable := tb
			if batch {
				go func() {
					worker, _ := createNewMigrator(client, from, to)
					remainingCount := worker.run(client, targetTable, round, currentOriginNode, concurrent)
					atomic.AddInt32(&totalRemainingReplica, int32(remainingCount))
					wg.Done()
				}()
			} else {
				remainingCount := nodesMigrator.run(client, targetTable, round, currentOriginNode, concurrent)
				atomic.AddInt32(&totalRemainingReplica, int32(remainingCount))
				wg.Done()
			}
		}
		wg.Wait()
		currentOriginNode = nodesMigrator.selectNextOriginNode()
		time.Sleep(10 * time.Second)
	}
}
