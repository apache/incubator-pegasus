package executor

import (
	"admin-cli/helper"
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/radmin"
)

func DiskMigrate(client *Client, replicaServer string, pidStr string, from string, to string, enableResolve bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if enableResolve {
		node, err := helper.Resolve(replicaServer, helper.Host2Addr)
		if err != nil {
			return err
		}
		replicaServer = node
	}

	pid, err := helper.Str2Gpid(pidStr)
	if err != nil {
		return err
	}

	replicaClient, err := client.GetReplicaClient(replicaServer)
	if err != nil {
		return err
	}

	resp, err := replicaClient.DiskMigrate(ctx, &radmin.ReplicaDiskMigrateRequest{
		Pid:        pid,
		OriginDisk: from,
		TargetDisk: to,
	})

	if err != nil {
		if resp != nil && resp.Hint != nil {
			return fmt.Errorf("Internal server error [%s:%s]", err, *resp.Hint)
		}
		return err
	}

	return nil
}

// TODO(jiashuo1) need generate migrate strategy(step) depends the disk-info result to run
func DiskBalance() error {
	fmt.Println("Wait support")
	return nil
}
