package nodesbalancer

import (
	"fmt"
	"time"

	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
)

func BalanceNodeCapacity(client *executor.Client, auto bool) error {
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
		checkCompleted(client, action)
		if !auto {
			break
		}
		time.Sleep(time.Second * 10)
	}
	return nil
}
