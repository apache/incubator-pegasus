package diskbalancer

import (
	"fmt"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
)

var WaitRunning = time.Second * 10  // time for wait migrate complete
var WaitCleaning = time.Second * 90 // time for wait garbage replica to clean complete

var ShortCleanInterval = "1"    // short time wait the disk cleaner clean garbage replica
var LongCleanInterval = "86400" // long time wait the disk cleaner clean garbage replica

// auto balance target node disk usage:
// -1. change the pegasus server disk cleaner internal for clean temp replica to free disk space in time
// -2. get the optimal migrate action to be ready to balance the disk until can't migrate base latest disk space stats
// -3. if current replica is `primary` status, force assign the replica to `secondary` status
// -4. migrate the replica base `getNextMigrateAction` result
// -5. loop query migrate progress using `DiskMigrate`, it will response `ERR_BUSY` if running
// -6. start next loop until can't allow to balance the node
// -7. recover disk cleaner internal if balance complete
// -8. set meta status to `lively` to balance primary and secondary // TODO(jiashuo1)
func BalanceDiskCapacity(client *executor.Client, replicaServer string, minSize int64, interval int, auto bool) error {
	WaitCleaning = time.Second * time.Duration(interval)

	if err := changeDiskCleanerInterval(client, replicaServer, ShortCleanInterval); err != nil {
		return err
	}
	defer func() {
		if err := changeDiskCleanerInterval(client, replicaServer, LongCleanInterval); err != nil {
			toolkits.LogWarn("revert disk cleaner failed")
		}
	}()

	for {
		action, err := getNextMigrateAction(client, replicaServer, minSize)
		if err != nil {
			return err
		}

		err = executor.DiskMigrate(client, replicaServer, action.replica.Gpid, action.from, action.to)
		if err != nil && action.replica.Status != "secondary" {
			err := forceAssignReplicaToSecondary(client, replicaServer, action.replica.Gpid)
			if err != nil {
				return err
			}
			time.Sleep(WaitRunning)
			continue
		}

		if err != nil {
			return fmt.Errorf("migrate(%s) start failed[auto=%v] err = %s", action.toString(), auto, err.Error())
		}

		toolkits.LogInfo(fmt.Sprintf("migrate(%s) has started[auto=%v], wait complete...\n", action.toString(), auto))
		for {
			// TODO(jiashuo1): using DiskMigrate RPC to query status, consider support queryDiskMigrateStatus RPC
			err = executor.DiskMigrate(client, replicaServer, action.replica.Gpid, action.from, action.to)
			if err == nil {
				time.Sleep(WaitRunning)
				continue
			}
			if strings.Contains(err.Error(), base.ERR_BUSY.String()) {
				toolkits.LogInfo(fmt.Sprintf("migrate(%s) is running, msg=%s, wait complete...\n", action.toString(), err.Error()))
				time.Sleep(WaitRunning)
				continue
			}
			toolkits.LogInfo(fmt.Sprintf("migrate(%s) is completed，result=%s, wait[%ds] disk cleaner remove garbage...\n\n",
				action.toString(), err.Error(), interval))
			break
		}

		if auto {
			time.Sleep(WaitCleaning)
			continue
		}

		time.Sleep(WaitCleaning)
		toolkits.LogInfo(fmt.Sprintf("you now disable auto-balance[auto=%v], stop and wait manual loop\n\n", auto))
		break
	}
	return nil
}
