package nodesbalancer

import (
	"fmt"
	"math"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	migrator "github.com/apache/incubator-pegasus/admin-cli/client"
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/diskbalancer"
	"github.com/apache/incubator-pegasus/admin-cli/util"
)

type NodesCapacity struct {
	Node      *util.PegasusNode `json:"node"`
	Disks     []executor.DiskCapacityStruct
	Total     int64 `json:"total"`
	Usage     int64 `json:"usage"`
	Available int64 `json:"available"`
}

type NodesReplica struct {
	Node     *util.PegasusNode
	Replicas []*executor.ReplicaCapacityStruct
}

type Migrator struct {
	CapacityLoad []NodesCapacity
	Total        int64
	Usage        int64
	Average      int64
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
		nodesLoad = append(nodesLoad, diskCapacity)
	}
	if nodesLoad == nil {
		return err
	}

	util.SortStructsByField(nodesLoad, "Usage")
	for _, node := range nodesLoad {
		m.CapacityLoad = append(m.CapacityLoad, node.(NodesCapacity))
		m.Total += node.(NodesCapacity).Total
		m.Usage += node.(NodesCapacity).Usage
	}
	m.Average = m.Usage / int64(len(nodesLoad))
	return nil
}

type partition struct {
	Gpid   *base.Gpid
	Status migrator.BalanceType
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

func (m *Migrator) selectNextAction(client *executor.Client) (*ActionProposal, error) {
	highNode := m.CapacityLoad[len(m.CapacityLoad)-1]
	lowNode := m.CapacityLoad[0]

	toolkits.LogInfo(fmt.Sprintf("expect_average = %dMB, high node = %s[usage=%dMB], low node = %s[usage=%dMB]\n",
		m.Average/1024, highNode.Node.String(), highNode.Usage/1024, lowNode.Node.String(), lowNode.Usage/1024))

	lowUsageRatio := lowNode.Usage * 100 / lowNode.Total
	highUsageRatio := highNode.Usage * 100 / highNode.Total

	if highUsageRatio-lowUsageRatio <= 5 {
		return nil, fmt.Errorf("high node and low node has little diff: %d vs %d", highUsageRatio, lowUsageRatio)
	}

	sizeAllowMoved := math.Min(float64(highNode.Usage-m.Average), float64(m.Average-lowNode.Usage)) / 1000

	highDiskOfHighNode := highNode.Disks[len(highNode.Disks)-1]
	highDiskReplicasOfHighNode, err := getDiskReplicas(client, &highNode, highDiskOfHighNode.Disk)
	if err != nil {
		return nil, fmt.Errorf("get high node[%s] high disk[%s] replicas err: %s", highNode.Node.String(), highDiskOfHighNode.Disk, err.Error())
	}

	totalReplicasOfLowNode, err := getNodeReplicas(client, &lowNode)
	if err != nil {
		return nil, fmt.Errorf("get low node[%s] replicas err: %s", lowNode.Node.String(), err.Error())
	}

	var selectReplica executor.ReplicaCapacityStruct
	for _, replica := range highDiskReplicasOfHighNode {
		if replica.Size > int64(sizeAllowMoved) {
			toolkits.LogDebug(fmt.Sprintf("select next replica for the replica is too large(replica_size > allow_size): %d > %f", replica.Size, sizeAllowMoved))
			continue
		}

		if totalReplicasOfLowNode.contain(replica.Gpid) {
			toolkits.LogDebug(fmt.Sprintf("select next replica for the replica(%s) is has existed target node(%s)", replica.Gpid, lowNode.Node.String()))
			continue
		}

		if selectReplica.Status == "primary" {
			return nil, fmt.Errorf("please downgrade origin node total replica as primary")
		}

		selectReplica = replica
	}

	if selectReplica.Gpid == "" {
		return nil, fmt.Errorf("can't find valid replica to balance")
	}

	gpid, err := util.Str2Gpid(selectReplica.Gpid)
	if err != nil {
		return nil, err
	}

	status := migrator.BalanceCopySec
	if selectReplica.Status == "primary" {
		status = migrator.BalanceCopyPri
	}
	return &ActionProposal{
		replica: &partition{
			Gpid:   gpid,
			Status: status,
		},
		from: &highNode,
		to:   &lowNode,
	}, err
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

func waitCompleted(client *executor.Client, action *ActionProposal) error {
	// set meta level as lively to clean garbage
	err := executor.SetMetaLevel(client, "lively")
	if err != nil {
		return err
	}

	for {
		replicas, err := getNodeReplicas(client, action.to)
		if err != nil {
			toolkits.LogInfo(err.Error())
			time.Sleep(time.Second * 10)
			continue
		}

		if !replicas.contain(fmt.Sprintf("%d.%d", action.replica.Gpid.Appid,
			action.replica.Gpid.PartitionIndex)) {
			toolkits.LogInfo(fmt.Sprintf("%s is running", action.toString()))
			time.Sleep(time.Second * 10)
			continue
		}
		break
	}

	// recover  meta level as steady to next action
	err = executor.SetMetaLevel(client, "steady")
	if err != nil {
		return err
	}
	return nil
}
