package aggregate

import (
	"context"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	log "github.com/sirupsen/logrus"
)

type pegasusClient struct {
	meta *session.MetaManager

	nodes map[string]*PerfClient
}

func (m *pegasusClient) listNodes() []*admin.NodeInfo {
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

func (m *pegasusClient) listTables() []*admin.AppInfo {
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

func (m *pegasusClient) updateNodes() {
	nodeInfos := m.listNodes()

	newNodes := make(map[string]*PerfClient)
	for _, n := range nodeInfos {
		addr := n.Address.GetAddress()
		node, found := m.nodes[addr]
		if !found {
			newNodes[addr] = NewPerfClient(addr)
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

func newClient(metaAddrs []string) *pegasusClient {
	return &pegasusClient{
		meta:  session.NewMetaManager(metaAddrs, session.NewNodeSession),
		nodes: make(map[string]*PerfClient),
	}
}
