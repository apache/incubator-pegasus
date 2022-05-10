package nodesbalancer

import (
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/diskbalancer"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"math"
)

type NodesCapacity struct {
	Node      *util.PegasusNode
	Disks     []executor.DiskCapacityStruct
	Total     int64
	Usage     int64
	Available int64
}

type Migrator struct {
	CapacityLoad []*NodesCapacity
	Total int64
}

func (m *Migrator) updateNodesCapacityLoad(client *executor.Client) error {
	nodes, err := client.Meta.ListNodes()
	if err != nil {
		return err
	}

	var nodesLoad []interface{}
	for _, node := range nodes {
		pegasusNode := client.Nodes.MustGetReplica(node.Address.GetAddress())
		disksLoad, totalUsage, totalCapacity, err := diskbalancer.QueryDiskCapacityInfo(client, pegasusNode.TCPAddr())
		if err != nil {
			return err
		}
		diskCapacity := NodesCapacity{
			Node:      pegasusNode,
			Disks:     disksLoad,
			Total:     totalCapacity,
			Usage:     totalUsage,
			Available: totalCapacity - totalUsage,
		}
		nodesLoad = append(nodesLoad, &diskCapacity)
	}
	if nodesLoad == nil {
		return err
	}

	util.SortStructsByField(nodesLoad, "Available")
	for _, node := range nodesLoad {
		m.CapacityLoad = append(m.CapacityLoad, node.(*NodesCapacity))
		m.Total += node.(*NodesCapacity).Total
	}

	return nil
}

type ActionProposal struct {
	replica executor.ReplicaCapacityStruct

}

func (m *Migrator) selectNextAction(client *executor.Client) {
	highNode := m.CapacityLoad[len(m.CapacityLoad) - 1]
	lowNode := m.CapacityLoad[0]

	averageCapacity := m.Total / int64(len(m.CapacityLoad))
	sizeCanMoved := math.Min(float64(highNode.Total - averageCapacity), float64(averageCapacity - lowNode.Total))
	if (lowNode.Available * 100 / lowNode.Total) - (highNode.Available * 100 / highNode.Total) <= 5 {
		return
	}

	highDisk := highNode.Disks[len(highNode.Disks) - 1]
	highDiskInfo, err := executor.GetDiskInfo(client, executor.CapacitySize, highNode.Node.TCPAddr(), "", highDisk.Disk, false)
	if err != nil {
		return
	}
	replicas, err := executor.ConvertReplicaCapacityStruct(highDiskInfo)
	if err != nil {
		return
	}

	targetNodeReplicas, err := getNodeReplica(client, lowNode)
	if err != nil {
		return
	}

	var selectReplica executor.ReplicaCapacityStruct
	for _, replica := range replicas {
		if replica.Size > int64(sizeCanMoved) {
			continue
		}

		if targetNodeReplicas.contain(replica) {
			continue
		}

		selectReplica = replica
	}

}

type replicas []executor.ReplicaCapacityStruct
func (r replicas) contain(selectReplica executor.ReplicaCapacityStruct) bool {
	for _, replica := range r {
		if replica.Gpid == selectReplica.Gpid {
			return true
		}
	}
	return false
}

func getNodeReplica(client *executor.Client, replicaServer *NodesCapacity) (replicas, error) {
	node := replicaServer.Node.TCPAddr()

	var totalDiskInfo []interface{}
	for _, disk := range replicaServer.Disks {
		tag := disk.Disk
		diskInfo, err := executor.GetDiskInfo(client, executor.CapacitySize, node, "", tag, false)
		if err != nil {
			return nil, err
		}
		totalDiskInfo = append(totalDiskInfo, diskInfo...)
	}
	replicas, err := executor.ConvertReplicaCapacityStruct(totalDiskInfo)
	if err != nil {
		return nil, err
	}
	return replicas, nil
}


