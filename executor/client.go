package executor

import (
	"io"

	"github.com/XiaoMi/pegasus-go-client/session"
)

// Client can access both Pegasus ReplicaServer and MetaServer.
type Client struct {
	io.Writer

	Meta *session.MetaManager

	ReplicaPool *session.ReplicaManager
}

// NewClient creates a client for accessing Pegasus cluster for use of admin-cli.
func NewClient(writer io.Writer, metaAddrs []string) *Client {
	return &Client{
		Writer:      writer,
		Meta:        session.NewMetaManager(metaAddrs, session.NewNodeSession),
		ReplicaPool: session.NewReplicaManager(session.NewNodeSession),
	}
}
