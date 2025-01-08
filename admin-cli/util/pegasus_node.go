/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package util

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/apache/incubator-pegasus/collector/aggregate"
	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/session"
)

// PegasusNode is a representation of MetaServer and ReplicaServer.
// Compared to session.NodeSession, it extends with more detailed information.
type PegasusNode struct {
	// the session is nil by default, it will be initialized only when needed.
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
// Will initialize the TCP connection.
func (n *PegasusNode) Replica() *session.ReplicaSession {
	if n.Type != session.NodeTypeReplica {
		panic(fmt.Sprintf("%s is not replica", n))
	}
	if n.session == nil {
		n.session = session.NewNodeSession(n.TCPAddr(), session.NodeTypeReplica)
	}
	return &session.ReplicaSession{NodeSession: n.session}
}

// Session returns a tcp session to the node.
// Will initialize the TCP connection.
func (n *PegasusNode) Session() session.NodeSession {
	if n.session == nil {
		n.session = session.NewNodeSession(n.TCPAddr(), session.NodeTypeReplica)
	}
	return n.session
}

func (n *PegasusNode) RPCAddress() *base.RPCAddress {
	return base.NewRPCAddress(n.IP, n.Port)
}

func (n *PegasusNode) Close() error {
	if n.session != nil {
		return n.session.Close()
	}
	return nil
}

// NewNodeFromTCPAddr creates a node from tcp address.
// NOTE:
//   - Will not initialize TCP connection unless needed.
//   - Should not be called too frequently because it costs 1 DNS resolution.
func NewNodeFromTCPAddr(addr string, ntype session.NodeType) *PegasusNode {
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
		n.Hostname = strings.TrimSuffix(hostnames[0], ".")
	}
}

// PegasusNodeManager manages the sessions of all types of Pegasus node.
type PegasusNodeManager struct {
	// filled on initialization, won't be updated after that.
	MetaAddresses []string

	// a cache for nodes, to prevent unnecessary hostname resolving
	// in each PegasusNode creation.
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
		n := NewNodeFromTCPAddr(addr, session.NodeTypeMeta)
		m.nodes[n.TCPAddr()] = n
	}
	for _, addr := range replicaAddrs {
		n := NewNodeFromTCPAddr(addr, session.NodeTypeReplica)
		m.nodes[n.TCPAddr()] = n
	}
	return m
}

// MustGetReplica returns a replica node even if it doens't exist before.
// User should assure the validity of the given info.
func (m *PegasusNodeManager) MustGetReplica(addr string) *PegasusNode {
	n, err := m.GetNode(addr, session.NodeTypeReplica)
	if err != nil {
		n = NewNodeFromTCPAddr(addr, session.NodeTypeReplica)
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

	if n, ok := m.nodes[addr]; ok { // node exists
		if n.Type != ntype {
			return nil, fmt.Errorf("node(%s) is not %s", n, ntype)
		}
		return n, nil
	}
	node, err := m.getNodeFromHost(addr, ntype)
	if err == nil {
		return node, nil
	}
	return nil, err
}

func (m *PegasusNodeManager) getNodeFromHost(hostPort string, ntype session.NodeType) (*PegasusNode, error) {
	for _, node := range m.nodes {
		if fmt.Sprintf("%s:%d", node.Hostname, node.Port) == hostPort {
			if node.Type != ntype {
				return nil, fmt.Errorf("node(%s) is not %s", node, ntype)
			}
			return node, nil
		}
	}

	return nil, fmt.Errorf("Invalid node %s", hostPort)
}

// GetAllNodes returns all nodes that matches the type. The result could be inconsistent
// with the latest cluster state. Please use MetaManager.ListNodes whenever possible.
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

func (m *PegasusNodeManager) GetPerfSession(addr string, ntype session.NodeType) *aggregate.PerfSession {
	node, err := m.GetNode(addr, ntype)
	if err != nil {
		panic(fmt.Sprintf("Get PerfSession %s error %s", addr, err))
	}

	return aggregate.WrapPerf(addr, node.session)
}

func (m *PegasusNodeManager) CloseAllNodes() error {
	var errorStrings []string
	for _, n := range m.nodes {
		err := n.Close()
		if err != nil {
			errorStrings = append(errorStrings, err.Error())
		}
	}
	if len(errorStrings) != 0 {
		return fmt.Errorf("%s", strings.Join(errorStrings, "\n"))
	}
	return nil
}
