package executor

import (
	"context"
	"fmt"
	"io"
	"time"

	adminClient "github.com/XiaoMi/pegasus-go-client/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/collector/aggregate"
)

// Client can access both Pegasus ReplicaServer and MetaServer.
type Client struct {
	io.Writer

	Meta *session.MetaManager

	ReplicaPool *session.ReplicaManager

	MetaAddresses []string

	ReplicaAddresses []string
}

// NewClient creates a client for accessing Pegasus cluster for use of admin-cli.
func NewClient(writer io.Writer, metaAddrs []string) *Client {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	meta := session.NewMetaManager(metaAddrs, session.NewNodeSession)
	replicaPool := session.NewReplicaManager(session.NewNodeSession)

	resp, err := meta.ListNodes(ctx, &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	})
	if err != nil {
		return nil
	}

	var nodes []string
	for _, node := range resp.Infos {
		nodes = append(nodes, node.Address.GetAddress())
	}

	return &Client{
		Writer:           writer,
		Meta:             meta,
		ReplicaPool:      replicaPool,
		MetaAddresses:    metaAddrs,
		ReplicaAddresses: nodes,
	}
}

func (client *Client) GetReplicaClient(addr string) (*session.ReplicaSession, error) {
	err := client.validateReplicaAddress(addr)
	if err != nil {
		return nil, err
	}
	return client.ReplicaPool.GetReplica(addr), nil
}

func (client *Client) GetRemoteCommandClient(addr string, nodeType session.NodeType) (*adminClient.RemoteCmdClient, error) {
	switch nodeType {
	case session.NodeTypeMeta:
		err := client.validateMetaAddress(addr)
		if err != nil {
			return nil, err
		}
	case session.NodeTypeReplica:
		err := client.validateReplicaAddress(addr)
		if err != nil {
			return nil, err
		}
	}
	return adminClient.NewRemoteCmdClient(addr, nodeType), nil
}

func (client *Client) GetPerfCounterClient(addr string) (*aggregate.PerfSession, error) {
	err := client.validateReplicaAddress(addr)
	if err != nil {
		return nil, err
	}
	return aggregate.NewPerfSession(addr), nil
}

func (client *Client) validateReplicaAddress(addr string) error {
	for _, node := range client.ReplicaAddresses {
		if node == addr {
			return nil
		}
	}
	return fmt.Errorf("The cluster doesn't exist the replica server node [%s]", addr)
}

// used for remote_command -t meta
func (client *Client) validateMetaAddress(addr string) error {
	for _, meta := range client.MetaAddresses {
		if addr == meta {
			return nil
		}
	}
	return fmt.Errorf("The cluster doesn't exist the meta server node [%s]", addr)
}
