package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

// ListAppEnvs command
func ListAppEnvs(c *Client, useTable string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := c.meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return err
	}

	for _, app := range resp.Infos {
		if app.AppName == useTable {
			outputBytes, err := json.MarshalIndent(app.Envs, "", "  ")
			if err != nil {
				return err
			}
			fmt.Fprintln(c, string(outputBytes))
			return nil
		}
	}
	return nil
}

// SetAppEnv command
func SetAppEnv(c *Client, useTable string, key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := c.meta.UpdateAppEnv(ctx, &admin.UpdateAppEnvRequest{
		Keys:   []string{key},
		Values: []string{value},
	})
	if err != nil {
		if resp != nil {
			return fmt.Errorf("error hint message: %s", resp.HintMessage)
		}
		return err
	}
	fmt.Fprintln(c, "ok")
	return nil
}
