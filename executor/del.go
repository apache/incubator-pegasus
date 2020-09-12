package executor

import (
	"context"
	"time"
)

func Del(rootCtx *Context, hashKeyStr, sortkeyStr string) error {
	pegasusArgs, err := readPegasusArgs(rootCtx, []string{hashKeyStr, sortkeyStr})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	return rootCtx.UseTable.Del(ctx, pegasusArgs[0], pegasusArgs[1])
}
