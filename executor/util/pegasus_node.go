package util

import (
	"fmt"
	"net"
	"sync"

	"github.com/XiaoMi/pegasus-go-client/session"
)

// PegasusNode is a representation of MetaServer and ReplicaServer, with address and node type.
// Compared to session.NodeSession, it extends with more detailed information.
type PegasusNode struct {
	// the session is nil by default, it will be initialized when needed.
	session session.NodeSession

	IP net.IP

	Port int

	Hostname string

	Type session.NodeType
}

// TCPAddr returns the tcp address of the node
func (n *PegasusNode) TCPAddr() string {
	return fmt.Sprintf("%s:%d", n.IP.String(), n.Port)
}

// CombinedAddr returns a string combining with tcp address and hostname.
func (n *PegasusNode) CombinedAddr() string {
	return fmt.Sprintf("%s(%s)", n.Hostname, n.TCPAddr())
}

func (n *PegasusNode) String() string {
	return fmt.Sprintf("[%s]%s", n.Type, n.CombinedAddr())
}

// Replica returns a ReplicaSession if this node is a ReplicaServer.
func (n *PegasusNode) Replica() *session.ReplicaSession {
	if n.Type != session.NodeTypeReplica {
		panic(fmt.Sprintf("%s is not replica", n))
	}
	if n.session != nil {
		n.session = session.NewNodeSession(n.TCPAddr(), session.NodeTypeReplica)
	}
	r, _ := n.session.(*session.ReplicaSession)
	return r
}

// Session returns a tcp session to the node.
func (n *PegasusNode) Session() session.NodeSession {
	if n.session != nil {
		n.session = session.NewNodeSession(n.TCPAddr(), session.NodeTypeReplica)
	}
	return n.session
}

// newNodeFromTCPAddr creates a node from tcp address.
func newNodeFromTCPAddr(addr string, ntype session.NodeType) *PegasusNode {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		// the addr given is always trusted
		panic(err)
	}

	n := &PegasusNode{
		IP:   tcpAddr.IP,
		Port: tcpAddr.Port,
		Type: ntype,
	}
	n.resolveIP()
	return n
}

func (n *PegasusNode) resolveIP() {
	hostnames, err := net.LookupAddr(n.IP.String())
	if err != nil {
		n.Hostname = "unknown"
	} else {
		n.Hostname = hostnames[0]
	}
}

// PegasusNodeManager manages the sessions of all types of Pegasus node.
type PegasusNodeManager struct {
	// filled on initialization, won't be updated after that.
	MetaAddresses []string

	mu               sync.RWMutex
	replicaAddresses []string
	nodes            map[string]*PegasusNode
}

// NewPegasusNodeManager creates a PegasusNodeManager.
func NewPegasusNodeManager(metaAddrs []string, replicaAddrs []string) *PegasusNodeManager {
	m := &PegasusNodeManager{
		MetaAddresses:    metaAddrs,
		replicaAddresses: replicaAddrs,
		nodes:            make(map[string]*PegasusNode),
	}
	for _, addr := range metaAddrs {
		m.nodes[addr] = newNodeFromTCPAddr(addr, session.NodeTypeMeta)
	}
	for _, addr := range replicaAddrs {
		m.nodes[addr] = newNodeFromTCPAddr(addr, session.NodeTypeReplica)
	}
	return m
}

// MustGetReplica returns a replica node even if it doens't exist before.
// User should assure the validity of the given info.
func (m *PegasusNodeManager) MustGetReplica(addr string) *PegasusNode {
	n, err := m.GetNode(addr, session.NodeTypeReplica)
	if err != nil {
		n = newNodeFromTCPAddr(addr, session.NodeTypeReplica)
		m.mu.Lock()
		m.nodes[addr] = n
		m.replicaAddresses = append(m.replicaAddresses, addr)
		m.mu.Unlock()
	}
	return n
}

// GetNode returns the specified node if it exists.
func (m *PegasusNodeManager) GetNode(addr string, ntype session.NodeType) (*PegasusNode, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var err error
	switch ntype {
	case session.NodeTypeMeta:
		err = m.validateReplicaAddress(addr)
	case session.NodeTypeReplica:
		err = m.validateReplicaAddress(addr)
	}
	if err != nil {
		return nil, err
	}

	// node that passes the validation above must exist in m.nodes
	return m.nodes[addr], nil
}

// GetAllNodes returns all nodes that matches the type.
func (m *PegasusNodeManager) GetAllNodes(ntype session.NodeType) []*PegasusNode {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*PegasusNode
	for _, n := range m.nodes {
		if n.Type == ntype {
			result = append(result, n)
		}
	}
	return result
}

func (m *PegasusNodeManager) validateReplicaAddress(addr string) error {
	for _, node := range m.replicaAddresses {
		if node == addr {
			return nil
		}
	}
	return fmt.Errorf("The cluster doesn't exist the replica server node [%s]", addr)
}

func (m *PegasusNodeManager) validateMetaAddress(addr string) error {
	for _, meta := range m.MetaAddresses {
		if addr == meta {
			return nil
		}
	}
	return fmt.Errorf("The cluster doesn't exist the meta server node [%s]", addr)
}
