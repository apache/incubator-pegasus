package admin

import (
	"context"
	"testing"

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
