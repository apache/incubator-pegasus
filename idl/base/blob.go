package base

import (
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
)

type Blob struct {
	Data []byte
}

func (b *Blob) Read(iprot thrift.TProtocol) error {
	data, err := iprot.ReadBinary()
	if err != nil {
		return err
	}
	b.Data = data
	return nil
}

func (b *Blob) Write(oprot thrift.TProtocol) error {
	return oprot.WriteBinary(b.Data)
}

func (b *Blob) String() string {
	if b == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Blob(%+v)", *b)
}
