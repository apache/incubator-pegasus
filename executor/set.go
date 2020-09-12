package executor

import (
	"context"
	"fmt"
	"time"
)

func Set(rootCtx *Context, hashKeyStr, sortkeyStr, valueStr string) error {
	pegasusArgs, err := readPegasusArgs(rootCtx, []string{hashKeyStr, sortkeyStr, valueStr})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	err = rootCtx.UseTable.Set(ctx, pegasusArgs[0], pegasusArgs[1], pegasusArgs[2])
	if err != nil {
		return err
	}
	fmt.Fprintln(rootCtx, "\nok")
	return nil
}
