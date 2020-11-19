package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/radmin"
)

func DiskMigrate(client *Client, replicaServer string, pidStr string, from string, to string, enableResolve bool) error {
	if enableResolve {
		node, err := resolve(replicaServer, Host2Addr)
		if err != nil {
			return err
		}
		replicaServer = node
	}

	pid, err := str2Gpid(pidStr)
	fmt.Println(pid)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*100000)
	defer cancel()
	// TODO(jiashuo1) update to resp, err := ... after fix err code
	resp, err := client.replicaPool.GetReplica(replicaServer).DiskMigrate(ctx, &radmin.ReplicaDiskMigrateRequest{
		Pid:        pid,
		OriginDisk: from,
		TargetDisk: to,
	})
	if err != nil {
		return err
	}

	return fmt.Errorf("Internal server error [%s:%s]", resp.Err.String(), *resp.Hint)

	/* TODO(jiashuo1) wait fix the err code
	if resp.Err != base.ERR_OK {
		return fmt.Errorf("Internal server error [%s]", resp.Err.String())
	}*/

	return nil
}

// TODO(jiashuo1) need generate migrate strategy(step) depends the disk-info result to run
func DiskBalance() error {
	fmt.Println("Wait support")
	return nil
}
