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
	gpid      *base.Gpid
	operation migrator.BalanceType
}

func (m *MigratorNode) downgradeAllReplicaToSecondary(client *executor.Client) {
	if m.checkIfNoPrimary(client) {
		return
	}

	for {
		err := migrator.MigratePrimariesOut(client.Meta, m.node)
		if err != nil {
			fmt.Printf("WARN:wait, migrate primary out of %s is invalid now, err = %s\n", m.String(), err)
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
		fmt.Printf("WARN:wait, migrate primary out of %s is invalid when list app, err = %s\n", m.String(), err)
		return false
	}

	for _, tb := range tables {
		partitions, err := migrator.ListPrimariesOnNode(client.Meta, m.node, tb.AppName)
		if err != nil {
			fmt.Printf("WARN:wait, migrate primary out of %s is invalid when list primaries, err = %s\n", m.String(), err)
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

func (m *MigratorNode) downgradeOneReplicaToSecondary(client *executor.Client, table string, gpid *base.Gpid) {
	for {
		if !m.contain(gpid) {
			fmt.Printf("ERROR: can't find replica[%s] on node[%s]\n", gpid.String(), m.node.String())
			time.Sleep(10 * time.Second)
			continue
		}

		resp, err := client.Meta.QueryConfig(table)
		if err != nil {
			fmt.Printf("WARN: wait, query config table[%s] for gpid[%s] now is invalid: %s\n", table, gpid.String(), err.Error())
			time.Sleep(10 * time.Second)
			continue
		}

		var selectReplica *replication.PartitionConfiguration
		for _, partition := range resp.Partitions {
			if partition.Pid.String() != gpid.String() {
				continue
			} else {
				selectReplica = partition
				break
			}
		}

		if selectReplica == nil {
			fmt.Printf("ERROR: can't find replica[%s] of table[%s]\n", gpid.String(), table)
			continue
		}

		if selectReplica.Primary.GetAddress() == m.node.TCPAddr() {
			secondaryNode := client.Nodes.MustGetReplica(selectReplica.Secondaries[0].GetAddress())
			err = client.Meta.Balance(gpid, migrator.BalanceMovePri, m.node, secondaryNode)
			if err != nil {
				fmt.Printf("WARN: wait, downgrade[%s] to secondary now is invalid: %s\n", gpid.String(), err.Error())
				time.Sleep(10 * time.Second)
				continue
			}
		} else {
			fmt.Printf("WARN: replica[%s] has been secondary on node[%s]\n", gpid.String(), m.node.String())
			return
		}

	}
}

func (m *MigratorNode) contain(gpid *base.Gpid) bool {
	for _, replica := range m.replicas {
		if replica.gpid.String() == gpid.String() {
			return true
		}
	}
	return false
}

func (m *MigratorNode) String() string {
	return m.node.String()
}
