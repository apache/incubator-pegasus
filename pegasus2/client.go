package pegasus2

import (
	"context"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/XiaoMi/pegasus-go-client/session"
)

type Client struct {
	metaMgr *session.MetaManager
}

func NewClient(cfg pegasus.Config) *Client {
	if len(cfg.MetaServers) == 0 {
		pegalog.GetLogger().Fatal("pegasus-go-client: meta sever list should not be empty")
		return nil
	}

	c := &Client{
		metaMgr: session.NewMetaManager(cfg.MetaServers, newNodeSession),
	}
	return c
}

func (p *Client) OpenTable(ctx context.Context, tableName string) (pegasus.TableConnector, error) {
	tb, err := func() (pegasus.TableConnector, error) {
		// Each table holds an independent set of replica sessions.
		// The meta sessions are shared by tables created from the same client.
		replicaMgr := session.NewReplicaManager(newNodeSession)
		return pegasus.ConnectTable(ctx, tableName, p.metaMgr, replicaMgr)
	}()
	return tb, pegasus.WrapError(err, pegasus.OpQueryConfig)
}

func (p *Client) Close() error {
	return p.metaMgr.Close()
}
