package nodesmigrator

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/util"
)

var GlobalBatchTable = false
var GlobalBatchTarget = false

func MigrateAllReplicaToNodes(client *executor.Client, from []string, to []string, tables []string, batchTable bool,
	batchTarget bool, targetCount int, concurrent int) error {
	GlobalBatchTable = batchTable
	GlobalBatchTarget = batchTarget
	if len(to) != targetCount {
		logPanic(fmt.Sprintf("please make sure the targets count == `--final_target` value： %d vs %d", len(to),
			targetCount))
	}

	logWarn(fmt.Sprintf("you now migrate to target count assign to be %d in final, "+
		"please make sure it is ok! sleep 10s and then start", targetCount))
	time.Sleep(time.Second * 10)

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
	currentTargetNodes := nodesMigrator.targets
	firstOrigin := currentOriginNode
	var totalRemainingReplica int32 = math.MaxInt32
	originRound := -1
	runTargetCount := 0

	balanceFactor := 0
	for {
		if totalRemainingReplica <= 0 {
			logInfo("\n\n==============completed for all origin nodes has been migrated================")
			return executor.ListNodes(client)
		}

		if currentOriginNode.String() == firstOrigin.String() {
			originRound++
		}
		logInfo(fmt.Sprintf("\n\n*******************[%d|%s]start migrate replicas, remainingReplica=%d*****************",
			balanceFactor, currentOriginNode.String(), totalRemainingReplica))

		currentOriginNode.downgradeAllReplicaToSecondary(client)
		if !GlobalBatchTarget {
			runTargetCount++
			target := nodesMigrator.selectNextTargetNode(nodesMigrator.targets)
			target.downgradeAllReplicaToSecondary(client)
			currentTargetNodes = []*util.PegasusNode{target.node}
		}

		if !GlobalBatchTarget {
			if runTargetCount/targetCount >= 1 {
				runTargetCount = 0
				balanceFactor++
			}
		} else {
			balanceFactor = originRound
		}

		totalRemainingReplica = 0
		tableCount := len(tableList)
		var wg sync.WaitGroup
		wg.Add(tableCount)
		for _, tb := range tableList {
			targetTable := tb
			if GlobalBatchTable {
				go func() {
					worker, _ := createNewMigrator(client, from, to)
					remainingCount := worker.run(client, targetTable, balanceFactor, currentOriginNode, currentTargetNodes, concurrent)
					atomic.AddInt32(&totalRemainingReplica, int32(remainingCount))
					wg.Done()
				}()
			} else {
				remainingCount := nodesMigrator.run(client, targetTable, balanceFactor, currentOriginNode, currentTargetNodes, concurrent)
				atomic.AddInt32(&totalRemainingReplica, int32(remainingCount))
				wg.Done()
			}
		}
		wg.Wait()
		currentOriginNode = nodesMigrator.selectNextOriginNode()
		time.Sleep(10 * time.Second)
	}
}
