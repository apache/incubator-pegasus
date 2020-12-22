package executor

import (
	"context"
	"fmt"
	"hash/crc64"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
)

var crc64Table = crc64.MakeTable(0x9a6c9329ac4bc9b5)

func crc64Hash(data []byte) uint64 {
	return crc64.Checksum(data, crc64Table)
}

func getPartitionIndex(hashKey []byte, partitionCount int) int32 {
	return int32(crc64Hash(hashKey) % uint64(partitionCount))
}

func PartitionIndex(rootCtx *Context, hashKeyStr string) error {
	pegasusArgs, err := readPegasusArgs(rootCtx, []string{hashKeyStr})
	if err != nil {
		return err
	}

	if rootCtx.UseTablePartitionCount == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := rootCtx.Meta.QueryConfig(ctx, rootCtx.UseTableName)
		if err != nil {
			return err
		}
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return fmt.Errorf("QueryConfig failed: %s", resp.GetErr().String())
		}
		rootCtx.UseTablePartitionCount = int(resp.PartitionCount)
	}

	partIdx := getPartitionIndex(pegasusArgs[0], rootCtx.UseTablePartitionCount)
	fmt.Fprintf(rootCtx, "\nPartition index : %d\n", partIdx)

	return nil
}
