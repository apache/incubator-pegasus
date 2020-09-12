package executor

import (
	"context"
	"fmt"
	"time"
)

func Get(rootCtx *Context, hashKeyStr, sortkeyStr string) error {
	pegasusArgs, err := readPegasusArgs(rootCtx, []string{hashKeyStr, sortkeyStr})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	rawValue, err := rootCtx.UseTable.Get(ctx, pegasusArgs[0], pegasusArgs[1])
	if err != nil {
		return err
	}
	if rawValue == nil {
		return fmt.Errorf("record not found\nHASH_KEY=%s\nSORT_KEY=%s", hashKeyStr, sortkeyStr)
	}
	value, err := rootCtx.ValueEnc.DecodeAll(rawValue)
	if err != nil {
		return err
	}
	fmt.Fprintf(rootCtx, "\n%s : %s : %s\n", hashKeyStr, sortkeyStr, value)
	return nil
}
