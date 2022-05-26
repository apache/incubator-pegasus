package base

import (
	"testing"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
)

func TestGpid(t *testing.T) {
	pegalog.SetLogger(pegalog.StderrLogger)
	type testCase struct {
		gpidObject *Gpid
		gpidBytes  []byte
	}

	testCases := []testCase{
		{&Gpid{
			Appid:          0,
			PartitionIndex: 0}, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		},
		{&Gpid{
			Appid:          0,
			PartitionIndex: 2}, []byte{0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0},
		},
		{&Gpid{
			Appid:          1,
			PartitionIndex: 0}, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
		},
		{&Gpid{
			Appid:          1,
			PartitionIndex: 2}, []byte{0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x1},
		},
	}

	for _, testCase := range testCases {
		// test gpid serialize
		buf := thrift.NewTMemoryBuffer()
		oprot := thrift.NewTBinaryProtocolTransport(buf)
		testCase.gpidObject.Write(oprot)
		oprot.WriteMessageEnd()
		assert.Equal(t, buf.Bytes(), testCase.gpidBytes)

		// test gpid deserialize
		iprot := thrift.NewTBinaryProtocolTransport(buf)
		readGpid := &Gpid{}
		readGpid.Read(iprot)
		iprot.ReadMessageEnd()
		assert.Equal(t, readGpid, testCase.gpidObject)
	}
}
