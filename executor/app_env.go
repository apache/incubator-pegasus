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
	return updateAppEnv(c, &admin.UpdateAppEnvRequest{
		Keys:    []string{key},
		Values:  []string{value},
		Op:      admin.AppEnvOperation_APP_ENV_OP_SET,
		AppName: useTable,
	})
}

// ClearAppEnv command
func ClearAppEnv(c *Client, useTable string) error {
	return updateAppEnv(c, &admin.UpdateAppEnvRequest{
		Op:          admin.AppEnvOperation_APP_ENV_OP_CLEAR,
		AppName:     useTable,
		ClearPrefix: new(string), /*empty prefix means clear of all*/
	})
}

// DelAppEnv command
// TODO(wutao): deleting a non-existed key returns "OK" now.
func DelAppEnv(c *Client, useTable string, key string, deletePrefix bool) error {
	if deletePrefix {
		return updateAppEnv(c, &admin.UpdateAppEnvRequest{
			Op:          admin.AppEnvOperation_APP_ENV_OP_CLEAR,
			AppName:     useTable,
			ClearPrefix: &key,
		})
	}
	return updateAppEnv(c, &admin.UpdateAppEnvRequest{
		Op:      admin.AppEnvOperation_APP_ENV_OP_DEL,
		Keys:    []string{key},
		AppName: useTable,
	})
}

func updateAppEnv(c *Client, req *admin.UpdateAppEnvRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := c.Meta.UpdateAppEnv(ctx, req)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("failed to update app envs:\n%s\nErrno:%s\nHint message:%s", req.String(), resp.Err.Errno, resp.HintMessage)
		}
		return err
	}
	fmt.Fprintln(c, "ok")
	return nil
}
