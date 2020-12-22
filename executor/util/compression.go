package util

import (
	"github.com/klauspost/compress/zstd"
)

// BytesCompression is an interface of algorithm for compressing bytes.
type BytesCompression interface {
	Compress([]byte) ([]byte, error)

	Decompress([]byte) ([]byte, error)
}

type zstdCompression struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func (z *zstdCompression) Compress(data []byte) ([]byte, error) {
	return z.encoder.EncodeAll(data, make([]byte, 0, len(data))), nil
}

func (z *zstdCompression) Decompress(data []byte) ([]byte, error) {
	return z.decoder.DecodeAll(data, nil)
}

// TODO(wutao): compression command is not implemented yet.
//
// func newZstdCompression() BytesCompression {
// 	decoder, _ := zstd.NewReader(nil)
// 	encoder, _ := zstd.NewWriter(nil)

// 	return &zstdCompression{
// 		encoder: encoder,
// 		decoder: decoder,
// 	}
// }
