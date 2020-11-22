package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

// GetMetaLevel command
func GetMetaLevel(c *Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := c.Meta.MetaControl(ctx, &admin.MetaControlRequest{
		Level: admin.MetaFunctionLevel_fl_invalid,
	})
	if err != nil {
		return err
	}

	var result = map[string]string{
		"meta_level": resp.OldLevel.String(),
	}
	outputBytes, _ := json.MarshalIndent(result, "", "  ")
	fmt.Fprintln(c, string(outputBytes))
	return nil
}

// SetMetaLevel command
func SetMetaLevel(c *Client, lvlStr string) error {
	metaLvl, err := admin.MetaFunctionLevelFromString("fl_" + strings.ToLower(lvlStr))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := c.Meta.MetaControl(ctx, &admin.MetaControlRequest{
		Level: metaLvl,
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(c, "Successfully update meta function level from \"%s\" to \"%s\"\n", resp.OldLevel.String(), metaLvl.String())
	return nil
}
