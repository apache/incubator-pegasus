package client

import "fmt"

// ReplicaNodesManager manages the sessions to replica nodes.
// NOTE: The methods are not-thread-safe.
type ReplicaNodesManager struct {

	// reuse client sessions
	nodes map[string]*RemoteCmdClient
}

// UpdateNodes
func (m *ReplicaNodesManager) UpdateNodes(nodes []*NodeInfo) {
	newNodes := make(map[string]*RemoteCmdClient)
	for _, n := range nodes {
		node, found := m.nodes[n.Addr]
		if !found {
			newNodes[n.Addr] = NewRemoteCmdClient(n.Addr)
		} else {
			newNodes[n.Addr] = node
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

// MustFindNode returns the node of address `addr`.
func (m *ReplicaNodesManager) MustFindNode(addr string) *RemoteCmdClient {
	node, found := m.nodes[addr]
	if !found {
		panic(fmt.Sprintf("unable to find node [%s]", addr))
	}
	return node
}

func NewReplicaNodesManager() *ReplicaNodesManager {
	return &ReplicaNodesManager{
		nodes: make(map[string]*RemoteCmdClient),
	}
}
