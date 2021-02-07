package session

import (
	"context"
	"testing"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestAdminRpcTypes_QueryBackupPolicy(t *testing.T) {
	defer leaktest.Check(t)()

	mm := NewMetaManager([]string{"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"}, NewNodeSession)
	defer mm.Close()
	resp, err := mm.QueryBackupPolicy(context.Background(), admin.NewQueryBackupPolicyRequest())

	assert.Nil(t, err)
	assert.Nil(t, resp.HintMsg)
	assert.Equal(t, len(resp.Policys), 0)
}
