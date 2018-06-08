package pegasus2

import (
	"context"
	"testing"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestClient_Get(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := pegasus.Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)

	assert.Nil(t, tb.Set(context.Background(), []byte("h1"), []byte("s1"), []byte("v1")))

	value, err := tb.Get(context.Background(), []byte("h1"), []byte("s1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("v1"), value)

	tb.Close()
	client.Close()
}
