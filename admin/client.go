package admin

import (
	"context"

	"github.com/XiaoMi/pegasus-go-client/session"
)

// Client provides the administration API to a specific cluster.
// Remember only the superusers configured to the cluster have the admin priviledges.
type Client interface {
	CreateTable(ctx context.Context, tableName string, partitionCount int) error

	DropTable(ctx context.Context, tableName string) error

	ListTables(ctx context.Context) ([]*TableInfo, error)
}

// TableInfo is the table information.
type TableInfo struct {
	Name string
}

type Config struct {
	MetaServers []string `json:"meta_servers"`
}

// NewClient returns an instance of Client.
func NewClient(cfg Config) Client {
	return &rpcBasedClient{
		metaManager: session.NewMetaManager(cfg.MetaServers, session.NewNodeSession),
	}
}

type rpcBasedClient struct {
	metaManager *session.MetaManager
}

func (c *rpcBasedClient) CreateTable(ctx context.Context, tableName string, partitionCount int) error {
	return c.metaManager.CreateTable(ctx, tableName, partitionCount)
}

func (c *rpcBasedClient) DropTable(ctx context.Context, tableName string) error {
	return c.metaManager.DropTable(ctx, tableName)
}

func (c *rpcBasedClient) ListTables(ctx context.Context) ([]*TableInfo, error) {
	appInfos, err := c.metaManager.ListTables(ctx)
	if err != nil {
		return nil, err
	}

	var results []*TableInfo
	for _, app := range appInfos {
		results = append(results, &TableInfo{Name: app.AppName})
	}
	return results, nil
}
