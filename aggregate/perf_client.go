package aggregate

import (
	"context"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/session"
	log "github.com/sirupsen/logrus"
)

// PerfClient manages sessions to all replica nodes.
type PerfClient struct {
	meta *session.MetaManager

	nodes map[string]*PerfSession
}

// GetPartitionStats retrieves all the partition stats from replica nodes.
func (m *PerfClient) GetPartitionStats() []*PartitionStats {
	m.updateNodes()

	partitions := make(map[base.Gpid]*PartitionStats)

	nodes := m.GetNodeStats("@")
	for _, n := range nodes {
		for name, value := range n.Stats {
			perfCounter := decodePartitionPerfCounter(name, value)
			if perfCounter == nil {
				continue
			}
			if !aggregatable(perfCounter) {
				continue
			}
			part := partitions[perfCounter.gpid]
			if part == nil {
				part = &PartitionStats{
					Gpid:  perfCounter.gpid,
					Stats: make(map[string]float64),
					Addr:  n.Addr,
				}
				partitions[perfCounter.gpid] = part
			}
			part.Stats[perfCounter.name] = perfCounter.value
		}
	}

	var ret []*PartitionStats
	for _, part := range partitions {
		extendStats(&part.Stats)
		ret = append(ret, part)
	}
	return ret
}

// NodeStat contains the stats of a replica node.
type NodeStat struct {
	// Address of the replica node.
	Addr string

	// perfCounter's name -> the value.
	Stats map[string]float64
}

// GetNodeStats retrieves all the stats matched with `filter` from replica nodes.
func (m *PerfClient) GetNodeStats(filter string) []*NodeStat {
	m.updateNodes()

	var ret []*NodeStat
	for _, n := range m.nodes {
		stat := &NodeStat{
			Addr:  n.Address,
			Stats: make(map[string]float64),
		}
		perfCounters, err := n.GetPerfCounters(filter)
		if err != nil {
			log.Errorf("unable to query perf-counters: %s", err)
			return nil
		}
		for _, p := range perfCounters {
			stat.Stats[p.Name] = p.Value
		}
		ret = append(ret, stat)
	}
	return ret
}

func (m *PerfClient) listNodes() []*admin.NodeInfo {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	resp, err := m.meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_ALIVE,
	})
	if err != nil {
		log.Error(err)
		return nil
	}
	return resp.Infos
}

func (m *PerfClient) listTables() []*admin.AppInfo {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	resp, err := m.meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		log.Error(err)
		return nil
	}
	return resp.Infos
}

func (m *PerfClient) updateNodes() {
	nodeInfos := m.listNodes()

	newNodes := make(map[string]*PerfSession)
	for _, n := range nodeInfos {
		addr := n.Address.GetAddress()
		node, found := m.nodes[addr]
		if !found {
			newNodes[addr] = NewPerfSession(addr)
		} else {
			newNodes[addr] = node
		}
	}
	for n, client := range m.nodes {
		// close the unused connections
		if _, found := newNodes[n]; !found {
			client.Close()
		}
	}
	m.nodes = newNodes
}

// NewPerfClient returns an instance of PerfClient.
func NewPerfClient(metaAddrs []string) *PerfClient {
	return &PerfClient{
		meta:  session.NewMetaManager(metaAddrs, session.NewNodeSession),
		nodes: make(map[string]*PerfSession),
	}
}
