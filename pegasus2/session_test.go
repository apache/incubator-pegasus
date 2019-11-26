package pegasus2

import (
	"context"
	"strings"
	"testing"
	"time"

	"encoding/binary"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/rrdb"
	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestNodeSession_SendTimeout(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*1)

	client := NewClient(pegasus.Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	})
	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()
	defer client.Close()

	// ensure deadline has expired.
	time.Sleep(time.Second)

	// send must timeout
	err = tb.Set(ctx, []byte("h1"), []byte("s1"), []byte("v1"))
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "send rpc timeout"))
}

// test the case: request is seqId:3, but response is seqId:1
func TestNodeSession_ReadStaleResponse(t *testing.T) {
	defer leaktest.Check(t)()

	// start echo server first
	n := newNodeSession("0.0.0.0:8800", session.NodeTypeMeta).(*nodeSession)
	defer n.Close()

	var expected []byte

	mockCodec := &session.MockCodec{}
	mockCodec.MockMarshal(func(v interface{}) ([]byte, error) {
		expected, _ = new(session.PegasusCodec).Marshal(v)
		buf := make([]byte, len(expected)+4)

		// prefixed with length
		binary.BigEndian.PutUint32(buf, uint32(len(buf)))
		copy(buf[4:], expected)

		return buf, nil
	})

	mockCodec.MockUnMarshal(func(data []byte, v interface{}) error {
		r, _ := v.(*session.PegasusRpcCall)
		r.SeqId = 1
		r.Name = "RPC_RRDB_RRDB_GET_ACK"
		r.Result = &rrdb.RrdbGetResult{
			Success: rrdb.NewReadResponse(),
		}
		return nil
	})

	n.codec = mockCodec

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*1000)

	n.seqId = 2
	args := &rrdb.RrdbGetArgs{Key: &base.Blob{Data: []byte("a")}}
	_, err := n.CallWithGpid(ctx, &base.Gpid{}, args, "RPC_RRDB_RRDB_GET")

	// ensure rpc timeout due to stale response
	assert.NotNil(t, err)
}
