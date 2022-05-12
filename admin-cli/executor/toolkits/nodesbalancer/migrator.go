package nodesbalancer

import (
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/apache/incubator-pegasus/admin-cli/client"
	migrator "github.com/apache/incubator-pegasus/admin-cli/client"
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/diskbalancer"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"math"
	"time"
)

type NodesCapacity struct {
	Node      *util.PegasusNode
	Disks     []executor.DiskCapacityStruct
	Total     int64
	Usage     int64
	Available int64
}

type NodesReplica struct {
	Node     *util.PegasusNode
	Replicas []*executor.ReplicaCapacityStruct
}

type Migrator struct {
	CapacityLoad  []*NodesCapacity
	Total         int64
	Average       int64
	BalancedCount int64
}

func BalanceNodeCapacity(client *executor.Client, auto bool) error {
	migrater := &Migrator{}
	for {
		err := migrater.updateNodesLoad(client)
		if err != nil {
			toolkits.LogInfo("retry update load")
			time.Sleep(time.Second * 10)
			continue
		}

		err, action := migrater.selectNextAction(client)
		err = client.Meta.Balance(action.replica.Gpid, action.replica.Status, action.from.Node, action.to.Node)
		if err != nil {
			return fmt.Errorf("migrate action[%s] now is invalid: %s", action.toString(), err.Error())
		}
		checkCompleted(client, action)
		if auto {
			break
		}
		time.Sleep(time.Second * 10)
	}
	return nil
}

func (m *Migrator) updateNodesLoad(client *executor.Client) error {
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
	m.Average = m.Total / int64(len(nodesLoad))

	for _, node := range m.CapacityLoad {
		if int64(math.Abs(float64(node.Usage-m.Average)))*100/m.Total <= 5 {
			m.BalancedCount++
		}
	}

	return nil
}

type partition struct {
	Gpid   *base.Gpid
	Status client.BalanceType
	Size   int64
}

type ActionProposal struct {
	replica *partition
	from    *NodesCapacity
	to      *NodesCapacity
}

func (act *ActionProposal) toString() string {
	return fmt.Sprintf("[%s]%s:%s=>%s", act.replica.Status.String(), act.replica.Gpid.String(),
		act.from.Node.String(), act.to.Node.String())
}

func (m *Migrator) selectNextAction(client *executor.Client) (error, *ActionProposal) {
	highNode := m.CapacityLoad[len(m.CapacityLoad)-1]
	lowNode := m.CapacityLoad[0]

	averageCapacity := m.Total / int64(len(m.CapacityLoad))
	sizeAllowMoved := math.Min(float64(highNode.Total-averageCapacity), float64(averageCapacity-lowNode.Total))
	if (lowNode.Available*100/lowNode.Total)-(highNode.Available*100/highNode.Total) <= 5 {
		return nil, nil
	}

	highDisk := highNode.Disks[len(highNode.Disks)-1]
	originNodeHighDiskReplicas, err := getDiskReplicas(client, highNode, highDisk.Disk)
	if err != nil {
		return err, nil
	}

	targetNodeTotalReplicas, err := getNodeReplicas(client, lowNode)
	if err != nil {
		return nil, nil
	}

	var selectReplica executor.ReplicaCapacityStruct
	for _, replica := range originNodeHighDiskReplicas {
		if replica.Size > int64(sizeAllowMoved) {
			continue
		}

		if targetNodeTotalReplicas.contain(replica.Gpid) {
			continue
		}

		if selectReplica.Status == "primary" {
			return fmt.Errorf("please downgrade primary"), nil
		}

		selectReplica = replica
	}

	gpid, err := util.Str2Gpid(selectReplica.Gpid)
	if err != nil {
		return err, nil
	}
	return nil, &ActionProposal{
		replica: &partition{
			Gpid:   gpid,
			Status: migrator.BalanceCopySec,
		},
		from: highNode,
		to:   lowNode,
	}
}

type replicas []executor.ReplicaCapacityStruct

func (r replicas) contain(selectReplica string) bool {
	for _, replica := range r {
		if replica.Gpid == selectReplica {
			return true
		}
	}
	return false
}

func getDiskReplicas(client *executor.Client, replicaServer *NodesCapacity, diskTag string) (replicas, error) {
	node := replicaServer.Node.TCPAddr()
	diskInfo, err := executor.GetDiskInfo(client, executor.CapacitySize, node, "", diskTag, false)
	if err != nil {
		return nil, err
	}
	replicas, err := executor.ConvertReplicaCapacityStruct(diskInfo)
	if err != nil {
		return nil, err
	}
	return replicas, nil
}

func getNodeReplicas(client *executor.Client, replicaServer *NodesCapacity) (replicas, error) {
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

func checkCompleted(client *executor.Client, action *ActionProposal) {
	for {
		replicas, err := getNodeReplicas(client, action.to)
		if err != nil {
			toolkits.LogInfo(err.Error())
			time.Sleep(time.Second * 10)
			continue
		}

		if !replicas.contain(action.replica.Gpid.String()) {
			toolkits.LogInfo(fmt.Sprintf("%s is running", action.toString()))
			time.Sleep(time.Second * 10)
			continue
		}
		break
	}

}
