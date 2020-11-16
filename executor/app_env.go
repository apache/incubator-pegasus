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
	resp, err := c.Meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return err
	}

	for _, app := range resp.Infos {
		if app.AppName == useTable {
			outputBytes, _ := json.MarshalIndent(app.Envs, "", "  ")
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
	resp, err := c.Meta.UpdateAppEnv(ctx, &admin.UpdateAppEnvRequest{
		Keys:    []string{key},
		Values:  []string{value},
		Op:      admin.AppEnvOperation_APP_ENV_OP_SET,
		AppName: useTable,
	})
	if err != nil {
		if resp != nil {
			return fmt.Errorf("failed to set app (\"%s\") with env \"%s\" => \"%s\" [%s]: %s", useTable, key, value, resp.Err.Errno, resp.HintMessage)
		}
		return err
	}
	fmt.Fprintln(c, "ok")
	return nil
}
