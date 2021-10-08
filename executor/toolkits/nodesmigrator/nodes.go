package nodesmigrator

import (
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	migrator "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/util"
)

type MigratorNode struct {
	node     *util.PegasusNode
	replicas []*Replica
}

type Replica struct {
	// todo: for ci pass, the variable is necessary in later pr
	part      *replication.PartitionConfiguration
	operation migrator.BalanceType
}

func (m *MigratorNode) downgradeAllReplicaToSecondary(client *executor.Client) {
	for {
		err := migrator.MigratePrimariesOut(client.Meta, m.node)
		if err != nil {
			fmt.Printf("WARN: wait, migrate primary out of %s is invalid now, err = %s\n", m.String(), err)
			time.Sleep(30 * time.Second)
			continue
		}
		time.Sleep(30 * time.Second)
		if !m.checkIfNoPrimary(client) {
			continue
		}
		return
	}
}

func (m *MigratorNode) checkIfNoPrimary(client *executor.Client) bool {
	tables, err := client.Meta.ListAvailableApps()
	if err != nil {
		fmt.Printf("WARN: wait, migrate primary out of %s is invalid when list app, err = %s\n", m.String(), err)
		return false
	}

	for _, tb := range tables {
		partitions, err := migrator.ListPrimariesOnNode(client.Meta, m.node, tb.AppName)
		if err != nil {
			fmt.Printf("WARN: wait, migrate primary out of %s is invalid when list primaries, err = %s\n", m.String(), err)
			return false
		}
		if len(partitions) > 0 {
			fmt.Printf("WARN: wait, migrate primary out of %s is not completed, current count = %d\n", m.String(), len(partitions))
			return false
		}
	}

	fmt.Printf("INFO: migrate primary out of %s successfully\n", m.String())
	return true
}

func (m *MigratorNode) String() string {
	return m.node.String()
}

func (m *MigratorNode) contain(gpid *base.Gpid) bool {
	// todo
	return true
}
