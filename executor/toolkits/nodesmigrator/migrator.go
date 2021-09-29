package nodesmigrator

import (
	"fmt"
	"math"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/util"
)

type Migrator struct {
	origins []*util.PegasusNode
	targets []*util.PegasusNode
}

func (m *Migrator) run(client *executor.Client, table string, round int, target *MigratorNode) int {
	for {
		remainingCount := m.getRemainingReplicaCount(client, table)
		if remainingCount <= 0 {
			fmt.Printf("INFO: [%s]completed for no replicas can be migrated\n", table)
			return remainingCount
		}

		validOriginNodes := m.getValidOriginNodes(client, table, target)
		if len(validOriginNodes) == 0 {
			fmt.Printf("INFO: [%s]no valid replicas can be migratede\n", table)
			return remainingCount
		}

		balanceCount := m.getReplicaCountIfBalanced(client, table)
		currentCount := m.getCurrentReplicaCountOnNode()
		if currentCount >= balanceCount {
			fmt.Printf("INFO: [%s]balance: no need migrate replicas to %s, currentCount=%d, expect=max(%d)\n",
				table, target.String(), currentCount, balanceCount)
			return remainingCount
		}

		maxConcurrentCount := int(math.Min(float64(len(validOriginNodes)), float64(balanceCount-currentCount)))
		m.submitMigrateTaskAndWait(client, table, validOriginNodes, target, maxConcurrentCount)
	}
}

func (m *Migrator) getCurrentTargetNode(index int) (int, *MigratorNode) {
	round := index/len(m.targets) + 1
	currentTargetNode := m.targets[index%len(m.targets)]
	return round, &MigratorNode{node: currentTargetNode}
}

func (m *Migrator) getCurrentReplicaCountOnNode() int {
	// todo
	return 0
}

func (m *Migrator) getRemainingReplicaCount(client *executor.Client, table string) int {
	// todo
	return 0
}

func (m *Migrator) getReplicaCountIfBalanced(client *executor.Client, table string) int {
	// todo
	return 0
}

func (m *Migrator) getValidOriginNodes(client *executor.Client, table string, target *MigratorNode) []*MigratorNode {
	//todo
	return nil
}

func (m *Migrator) submitMigrateTaskAndWait(client *executor.Client, table string, origins []*MigratorNode,
	target *MigratorNode, maxConcurrentCount int) {
	//todo
}

func createNewMigrator(client *executor.Client, from []string, to []string) (*Migrator, error) {
	origins, targets, err := convert2MigratorNodes(client, from, to)
	if err != nil {
		return nil, fmt.Errorf("invalid origin or target node, error = %s", err.Error())
	}

	return &Migrator{
		origins: origins,
		targets: targets,
	}, nil
}

func convert2MigratorNodes(client *executor.Client, from []string, to []string) ([]*util.PegasusNode, []*util.PegasusNode, error) {
	origins, err := convert(client, from)
	if err != nil {
		return nil, nil, err
	}
	targets, err := convert(client, to)
	if err != nil {
		return nil, nil, err
	}
	return origins, targets, nil
}

func convert(client *executor.Client, nodes []string) ([]*util.PegasusNode, error) {
	var pegasusNodes []*util.PegasusNode
	for _, addr := range nodes {
		node, err := client.Nodes.GetNode(addr, session.NodeTypeReplica)
		if err != nil {
			return nil, fmt.Errorf("list node failed: %s", err)
		}
		pegasusNodes = append(pegasusNodes, node)
	}
	if pegasusNodes == nil {
		return nil, fmt.Errorf("invalid nodes list")
	}
	return pegasusNodes, nil
}
