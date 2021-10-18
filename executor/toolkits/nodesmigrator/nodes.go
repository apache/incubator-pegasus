package nodesmigrator

import (
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	migrator "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/util"
)

type MigratorNode struct {
	node     *util.PegasusNode
	replicas []*Replica
}

type Replica struct {
	gpid      *base.Gpid
	operation migrator.BalanceType
}

func (r *Replica) String() string {
	return fmt.Sprintf("%s|%s", r.gpid.String(), r.operation.String())
}

func (m *MigratorNode) downgradeAllReplicaToSecondary(client *executor.Client) {
	if m.checkIfNoPrimary(client) {
		return
	}

	for {
		err := migrator.MigratePrimariesOut(client.Meta, m.node)
		if err != nil {
			logWarn(fmt.Sprintf("WARN: migrate primary out of %s is invalid now, err = %s\n", m.String(), err), false)
			time.Sleep(10 * time.Second)
			continue
		}

		for {
			time.Sleep(10 * time.Second)
			if !m.checkIfNoPrimary(client) {
				continue
			}
			return
		}
	}
}

func (m *MigratorNode) checkIfNoPrimary(client *executor.Client) bool {
	tables, err := client.Meta.ListAvailableApps()
	if err != nil {
		logWarn(fmt.Sprintf("WARN:migrate primary out of %s is invalid when list app, err = %s", m.String(), err), false)
		return false
	}

	for _, tb := range tables {
		partitions, err := migrator.ListPrimariesOnNode(client.Meta, m.node, tb.AppName)
		if err != nil {
			logWarn(fmt.Sprintf("WARN:migrate primary out of %s is invalid when list primaries, err = %s", m.String(), err), false)
			return false
		}
		if len(partitions) > 0 {
			logWarn(fmt.Sprintf("WARN: migrate primary out of %s is not completed, current count = %d", m.String(), len(partitions)), false)
			return false
		}
	}

	logInfo(fmt.Sprintf("INFO: migrate primary out of %s successfully", m.String()), true)
	return true
}

func (m *MigratorNode) contain(gpid *base.Gpid) bool {
	for _, replica := range m.replicas {
		if replica.gpid.String() == gpid.String() {
			return true
		}
	}
	return false
}

func (m *MigratorNode) concurrent(acts *MigrateActions) int {
	return acts.getConcurrent(m)
}

func (m *MigratorNode) primaryCount() int {
	count := 0
	for _, replica := range m.replicas {
		if replica.operation == migrator.BalanceCopyPri {
			count++
		}
	}
	return count
}

func (m *MigratorNode) String() string {
	return m.node.String()
}
