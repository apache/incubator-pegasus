package diskbalancer

import (
	"fmt"
	"math"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	adminClient "github.com/apache/incubator-pegasus/admin-cli/client"
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
	"github.com/apache/incubator-pegasus/admin-cli/util"
)

type DiskStats struct {
	diskCapacity    executor.DiskCapacityStruct
	replicaCapacity []executor.ReplicaCapacityStruct
}

type MigrateDisk struct {
	averageUsage int64
	currentNode  string
	highDisk     DiskStats
	lowDisk      DiskStats
}

func (m *MigrateDisk) toString() string {
	return fmt.Sprintf("Node=%s, highDisk=%s[%dMB(%d%%)]=>lowDisk=%s[%dMB(%d%%)]", m.currentNode,
		m.highDisk.diskCapacity.Disk, m.highDisk.diskCapacity.Usage, m.highDisk.diskCapacity.Ratio,
		m.lowDisk.diskCapacity.Disk, m.lowDisk.diskCapacity.Usage, m.lowDisk.diskCapacity.Ratio)
}

type MigrateAction struct {
	node    string
	replica executor.ReplicaCapacityStruct
	from    string
	to      string
}

func (m *MigrateAction) toString() string {
	return fmt.Sprintf("node=%s, replica=%s, %s=>%s", m.node, m.replica.Gpid, m.from, m.to)
}

func changeDiskCleanerInterval(client *executor.Client, replicaServer string, cleanInterval string) error {
	toolkits.LogInfo(fmt.Sprintf("set gc_disk_migration_origin_replica_interval_seconds = %ss ", cleanInterval))
	err := executor.ConfigCommand(client, session.NodeTypeReplica, replicaServer,
		"gc_disk_migration_origin_replica_interval_seconds", "set", cleanInterval)
	return err
}

func getNextMigrateAction(client *executor.Client, replicaServer string, minSize int64) (*MigrateAction, error) {
	disks, totalUsage, totalCapacity, err := queryDiskCapacityInfo(client, replicaServer)
	if err != nil {
		return nil, err
	}
	diskMigrateInfo, err := getMigrateDiskInfo(client, replicaServer, disks, totalUsage, totalCapacity)
	if err != nil {
		return nil, err
	}

	migrateAction, err := computeMigrateAction(diskMigrateInfo, minSize)
	if err != nil {
		return nil, err
	}
	return migrateAction, nil
}

func queryDiskCapacityInfo(client *executor.Client, replicaServer string) ([]executor.DiskCapacityStruct, int64, int64, error) {
	diskCapacityOnNode, err := executor.GetDiskInfo(client, executor.CapacitySize, replicaServer, "", "", false)
	if err != nil {
		return nil, 0, 0, err
	}
	util.SortStructsByField(diskCapacityOnNode, "Usage")
	var disks []executor.DiskCapacityStruct
	var totalUsage int64
	var totalCapacity int64
	for _, disk := range diskCapacityOnNode {
		if s, ok := disk.(executor.DiskCapacityStruct); ok {
			disks = append(disks, s)
			totalUsage += s.Usage
			totalCapacity += s.Capacity
		} else {
			return nil, 0, 0, fmt.Errorf("can't covert to DiskCapacityStruct")
		}
	}

	if disks == nil {
		return nil, 0, 0, fmt.Errorf("the node(%s) has no ssd", replicaServer)
	}
	if len(disks) == 1 {
		return nil, 0, 0, fmt.Errorf("the node(%s) only has one disk, can't balance", replicaServer)
	}

	return disks, totalUsage, totalCapacity, nil
}

func getMigrateDiskInfo(client *executor.Client, replicaServer string, disks []executor.DiskCapacityStruct,
	totalUsage int64, totalCapacity int64) (*MigrateDisk, error) {
	highUsageDisk := disks[len(disks)-1]
	highDiskInfo, err := executor.GetDiskInfo(client, executor.CapacitySize, replicaServer, "", highUsageDisk.Disk, false)
	if err != nil {
		return nil, err
	}
	lowUsageDisk := disks[0]
	lowDiskInfo, err := executor.GetDiskInfo(client, executor.CapacitySize, replicaServer, "", lowUsageDisk.Disk, false)
	if err != nil {
		return nil, err
	}

	if highUsageDisk.Ratio < 10 {
		return nil, fmt.Errorf("no need balance since the high disk still enough capacity(balance threshold=10%%): "+
			"high(%s): %dMB(%d%%); low(%s): %dMB(%d%%)", highUsageDisk.Disk, highUsageDisk.Usage,
			highUsageDisk.Ratio, lowUsageDisk.Disk, lowUsageDisk.Usage, lowUsageDisk.Ratio)
	}

	averageUsage := totalUsage / int64(len(disks))
	averageRatio := totalUsage * 100 / totalCapacity
	if highUsageDisk.Ratio-lowUsageDisk.Ratio < 5 {
		return nil, fmt.Errorf("no need balance since the disk is balanced:"+
			" high(%s): %dMB(%d%%); low(%s): %dMB(%d%%); average: %dMB(%d%%)",
			highUsageDisk.Disk, highUsageDisk.Usage, highUsageDisk.Ratio, lowUsageDisk.Disk,
			lowUsageDisk.Usage, lowUsageDisk.Ratio, averageUsage, averageRatio)
	}

	replicaCapacityOnHighDisk, err := executor.ConvertReplicaCapacityStruct(highDiskInfo)
	if err != nil {
		return nil, fmt.Errorf("parse replica info on high disk(%s) failed: %s", highUsageDisk.Disk, err.Error())
	}
	replicaCapacityOnLowDisk, err := executor.ConvertReplicaCapacityStruct(lowDiskInfo)
	if err != nil {
		return nil, fmt.Errorf("parse replica info on low disk(%s) failed: %s", highUsageDisk.Disk, err.Error())
	}
	return &MigrateDisk{
		averageUsage: averageUsage,
		currentNode:  replicaServer,
		highDisk: DiskStats{
			diskCapacity:    highUsageDisk,
			replicaCapacity: replicaCapacityOnHighDisk,
		},
		lowDisk: DiskStats{
			diskCapacity:    lowUsageDisk,
			replicaCapacity: replicaCapacityOnLowDisk,
		},
	}, nil
}

func computeMigrateAction(migrate *MigrateDisk, minSize int64) (*MigrateAction, error) {
	lowDiskCanReceiveMax := migrate.averageUsage - migrate.lowDisk.diskCapacity.Usage
	highDiskCanSendMax := migrate.highDisk.diskCapacity.Usage - migrate.averageUsage
	sizeNeedMove := int64(math.Min(float64(lowDiskCanReceiveMax), float64(highDiskCanSendMax)))

	var selectReplica *executor.ReplicaCapacityStruct
	for i := len(migrate.highDisk.replicaCapacity) - 1; i >= 0; i-- {
		if migrate.highDisk.replicaCapacity[i].Size > sizeNeedMove {
			continue
		} else {
			selectReplica = &migrate.highDisk.replicaCapacity[i]
			break
		}
	}

	if selectReplica == nil {
		return nil, fmt.Errorf("can't balance(%s): high disk min replica(%s) size(%dMB) must <= sizeNeedMove(%dMB)",
			migrate.toString(),
			migrate.highDisk.replicaCapacity[0].Gpid,
			migrate.highDisk.replicaCapacity[0].Size,
			sizeNeedMove)
	}

	// if select replica size is too small, it will need migrate many replica and result in `replica count not balance` among disk
	if selectReplica.Size < minSize {
		return nil, fmt.Errorf("not suggest balance(%s): the qualified(must<=sizeNeedMove(%dMB)) replica size(%s=%dMB) must >= minSize(%dMB))",
			migrate.toString(), sizeNeedMove, selectReplica.Gpid, selectReplica.Size, minSize)
	}

	fmt.Printf("ACTION:disk migrate(sizeNeedMove=%dMB): %s, gpid(%s)=%s(%dMB)\n",
		sizeNeedMove, migrate.toString(), selectReplica.Status, selectReplica.Gpid, selectReplica.Size)

	return &MigrateAction{
		node:    migrate.currentNode,
		replica: *selectReplica,
		from:    migrate.highDisk.diskCapacity.Disk,
		to:      migrate.lowDisk.diskCapacity.Disk,
	}, nil
}

func forceAssignReplicaToSecondary(client *executor.Client, replicaServer string, gpid string) error {
	fmt.Printf("WARNING: the select replica is not secondary, will force assign it secondary\n")
	if _, err := client.Meta.MetaControl(admin.MetaFunctionLevel_fl_steady); err != nil {
		return err
	}
	secondaryNode, err := getReplicaSecondaryNode(client, gpid)
	if err != nil {
		return err
	}
	replica, err := util.Str2Gpid(gpid)
	if err != nil {
		return err
	}
	return client.Meta.Balance(replica, adminClient.BalanceMovePri,
		util.NewNodeFromTCPAddr(replicaServer, session.NodeTypeReplica), secondaryNode)
}

func getReplicaSecondaryNode(client *executor.Client, gpid string) (*util.PegasusNode, error) {
	replica, err := util.Str2Gpid(gpid)
	if err != nil {
		return nil, err
	}
	tables, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
	if err != nil {
		return nil, fmt.Errorf("can't get the table name of replica %s when migrate the replica", gpid)
	}
	var tableName string
	for _, tb := range tables {
		if tb.AppID == replica.Appid {
			tableName = tb.AppName
			break
		}
	}
	if tableName == "" {
		return nil, fmt.Errorf("can't find the table for %s when migrate the replica", gpid)
	}

	resp, err := client.Meta.QueryConfig(tableName)
	if err != nil {
		return nil, fmt.Errorf("can't get the table %s configuration when migrate the replica(%s): %s",
			tableName, gpid, err)
	}

	var secondaryNode *util.PegasusNode
	for _, partition := range resp.Partitions {
		if partition.Pid.String() == replica.String() {
			secondaryNode = util.NewNodeFromTCPAddr(partition.Secondaries[0].GetAddress(), session.NodeTypeReplica)
		}
	}

	if secondaryNode == nil {
		return nil, fmt.Errorf("can't get the replica %s secondary node", gpid)
	}
	return secondaryNode, nil
}
