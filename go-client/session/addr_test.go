package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveMetaAddr(t *testing.T) {
	addrs := []string{"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"}
	resolvedAddrs, err := ResolveMetaAddr(addrs)
	assert.Nil(t, err)
	assert.Equal(t, addrs, resolvedAddrs)

	addrs = []string{"127.0.0.1:34601", "www.baidu.com", "127.0.0.1:34603"}
	_, err = ResolveMetaAddr(addrs)
	assert.NotNil(t, err)

	addrs = []string{"www.baidu.com"}
	_, err = ResolveMetaAddr(addrs)
	assert.Nil(t, err)
	assert.Greater(t, len(addrs), 0)

	addrs = []string{"abcde"}
	_, err = ResolveMetaAddr(addrs)
	assert.NotNil(t, err)

	addrs = nil
	_, err = ResolveMetaAddr(addrs)
	assert.NotNil(t, err)

	addrs = []string{}
	_, err = ResolveMetaAddr(addrs)
	assert.NotNil(t, err)
}
