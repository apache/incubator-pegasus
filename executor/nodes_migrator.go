package executor

import (
	"fmt"
	"math"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/util"
)

var targetNodeIndex = 0

// return the target node and the round index(the migrate may be execute multi round)
func getTargetNode(targets []*util.PegasusNode) (*util.PegasusNode, int) {
	round := targetNodeIndex/len(targets) + 1
	node := targets[targetNodeIndex%len(targets)]
	return node, round
}

func convert2PegasusNodeStruct(client *Client, from []string, to []string) ([]*util.PegasusNode, []*util.PegasusNode, error) {
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

func convert(client *Client, nodes []string) ([]*util.PegasusNode, error) {
	var pegasusNodes []*util.PegasusNode
	for _, addr := range nodes {
		n, err := client.Nodes.GetNode(addr, session.NodeTypeReplica)
		if err != nil {
			return nil, fmt.Errorf("list node failed: %s", err)
		}
		pegasusNodes = append(pegasusNodes, n)
	}
	if pegasusNodes == nil {
		return nil, fmt.Errorf("invalid nodes list")
	}
	return pegasusNodes, nil
}

func MigrateAllReplicaToNodes(client *Client, period int64, from []string, to []string) error {
	origins, targets, err := convert2PegasusNodeStruct(client, from, to)
	if err != nil {
		return fmt.Errorf("invalid origin or target node, error = %s", err.Error())
	}

	tables, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
	if err != nil {
		return fmt.Errorf("list app failed: %s", err.Error())
	}

	var remainingReplica = math.MaxInt16
	for {
		if remainingReplica <= 0 {
			fmt.Printf("INFO: completed for all the targets has migrate\n")
			return ListNodes(client)
		}
		currentTargetNode, round := getTargetNode(targets)
		fmt.Printf("\n\n********[%d]start migrate replicas to %s******\n", round, currentTargetNode.String())
		fmt.Printf("INFO: migrate out all primary from current node %s\n", currentTargetNode.String())
		// assign all primary replica to secondary on target node to avoid read influence
		migratePrimariesOut(client, currentTargetNode)
		tableCompleted := 0
		for {
			// migrate enough replica to one target node per round.
			// pick next node if all tables have been handled completed.
			if tableCompleted >= len(tables) {
				targetNodeIndex++
				break
			}
			tableCompleted = 0
			remainingReplica = 0
			for _, tb := range tables {
				needMigrateReplicaCount, currentNodeHasBalanced, validOriginNode :=
					migrateReplicaPerTable(client, round, tb.AppName, origins, targets, currentTargetNode)
				remainingReplica = remainingReplica + needMigrateReplicaCount
				// table migrate completed if all replicas have been migrated or
				// target node has been balanced or
				// origin nodes has no valid replica can be migrated
				if needMigrateReplicaCount <= 0 || currentNodeHasBalanced || validOriginNode == 0 {
					tableCompleted++
					continue
				}
			}
			time.Sleep(10 * time.Second)
		}
	}
}

func migratePrimariesOut(client *Client, node *util.PegasusNode) {
	//todo
}

func migrateReplicaPerTable(client *Client, round int, table string, origins []*util.PegasusNode,
	targets []*util.PegasusNode, currentTargetNode *util.PegasusNode) (int, bool, int) {
	//todo
	return 0, false, 0
}
