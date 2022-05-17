package nodesbalancer

import (
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/session"
	"time"

	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
)

func BalanceNodeCapacity(client *executor.Client, auto bool) error {
	err := initClusterEnv(client)
	if err != nil {
		return err
	}

	balancer := &Migrator{}
	for {
		err := balancer.updateNodesLoad(client)
		if err != nil {
			toolkits.LogInfo(fmt.Sprintf("retry update load, err = %s", err.Error()))
			time.Sleep(time.Second * 10)
			continue
		}

		err, action := balancer.selectNextAction(client)
		if err != nil {
			return err
		}
		err = client.Meta.Balance(action.replica.Gpid, action.replica.Status, action.from.Node, action.to.Node)
		if err != nil {
			return fmt.Errorf("migrate action[%s] now is invalid: %s", action.toString(), err.Error())
		}
		waitCompleted(client, action)
		if !auto {
			break
		}
		time.Sleep(time.Second * 10)
	}
	return nil
}

func initClusterEnv(client *executor.Client) error {
	// set meta level as steady
	toolkits.LogWarn("This cluster will be balanced based capacity, please don't open count-balance in later")
	time.Sleep(time.Second * 3)
	err := executor.SetMetaLevel(client, "steady")
	if err != nil {
		return err
	}
	// disable migrate replica base `lively`
	err = executor.RemoteCommand(client, session.NodeTypeReplica, "", "meta.lb.only_move_primary", []string{"true"})
	if err != nil {
		return err
	}
	err = executor.RemoteCommand(client, session.NodeTypeReplica, "", "meta.lb.only_primary_balancer", []string{"true"})
	if err != nil {
		return err
	}
	// reset garbage replica clear interval
	err = executor.ConfigCommand(client, session.NodeTypeReplica, "", "gc_disk_error_replica_interval_seconds", "set", "10")
	if err != nil {
		return err
	}
	err = executor.ConfigCommand(client, session.NodeTypeReplica, "", "gc_disk_garbage_replica_interval_seconds", "set", "10")
	if err != nil {
		return err
	}
	return nil
}
