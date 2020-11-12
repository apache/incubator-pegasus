package admin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAdmin_Table(t *testing.T) {
	c := NewClient(Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	})

	hasTable := func(tables []*TableInfo, tableName string) bool {
		for _, tb := range tables {
			if tb.Name == tableName {
				return true
			}
		}
		return false
	}

	err := c.DropTable(context.Background(), "admin_table_test")
	assert.Nil(t, err)

	// no such table after deletion
	tables, err := c.ListTables(context.Background())
	assert.Nil(t, err)
	assert.False(t, hasTable(tables, "admin_table_test"))

	err = c.CreateTable(context.Background(), "admin_table_test", 16)
	assert.Nil(t, err)

	tables, err = c.ListTables(context.Background())
	assert.Nil(t, err)
	assert.True(t, hasTable(tables, "admin_table_test"))

	err = c.DropTable(context.Background(), "admin_table_test")
	assert.Nil(t, err)
}

func TestAdmin_ListTablesTimeout(t *testing.T) {
	c := NewClient(Config{
		MetaServers: []string{"0.0.0.0:123456"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := c.ListTables(ctx)
	assert.Equal(t, err, context.DeadlineExceeded)
}

func TestAdmin_GetAppEnvs(t *testing.T) {
	c := NewClient(Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	})

	tables, err := c.ListTables(context.Background())
	assert.Nil(t, err)
	for _, tb := range tables {
		assert.Empty(t, tb.Envs)
	}
}
