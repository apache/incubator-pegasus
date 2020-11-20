package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/radmin"
)

func DiskMigrate(client *Client, replicaServer string, pidStr string, from string, to string, enableResolve bool) error {
	if enableResolve {
		node, err := Resolve(replicaServer, Host2Addr)
		if err != nil {
			return err
		}
		replicaServer = node
	}

	pid, err := Str2Gpid(pidStr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.ReplicaPool.GetReplica(replicaServer).DiskMigrate(ctx, &radmin.ReplicaDiskMigrateRequest{
		Pid:        pid, // TODO(jiashuo1) server parser pid is error, need fix
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
