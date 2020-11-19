package admin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/XiaoMi/pegasus-go-client/session"
)

func TestAdmin_RemoteCommandCall(t *testing.T) {

	c := NewRemoteCmdClient("127.0.0.1:34801", session.NodeTypeReplica)
	resp, err := c.Call(context.Background(), "help", []string{})
	assert.Nil(t, err)
	assert.NotEmpty(t, resp)
}
