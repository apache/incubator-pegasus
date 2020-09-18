package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoteCmdClientCall(t *testing.T) {
	client := NewRemoteCmdClient("127.0.0.1:34601")
	res, err := client.call("help", []string{})
	assert.Nil(t, err)
	assert.NotEqual(t, res, "")
}

func TestRemoteCmdClientPerfCounter(t *testing.T) {
	client := NewRemoteCmdClient("127.0.0.1:34601")
	res, err := client.GetPerfCounters("RPC_RRDB_RRDB_INCR")
	assert.Nil(t, err)
	assert.Greater(t, len(res), 0)
}
