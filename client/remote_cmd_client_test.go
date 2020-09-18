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
