// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegatest

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
)

type Onebox struct {
	ScriptPath string
}

func (ob *Onebox) StartOnebox() error {
	return ob.execute("start_onebox")
}

func (ob *Onebox) StopOnebox() error {
	return ob.execute("stop_onebox")
}

func (ob *Onebox) StopReplica(replicaId int) error {
	return ob.execute("stop_onebox_instance", "-r", strconv.Itoa(replicaId))
}

func (ob *Onebox) StopMeta(metaId int) error {
	return ob.execute("stop_onebox_instance", "-m", strconv.Itoa(metaId))
}

func (ob *Onebox) ClearOnebox() error {
	return ob.execute("clear_onebox")
}

func (ob *Onebox) execute(args ...string) error {
	scriptDir := filepath.Dir(ob.ScriptPath)

	cmdstr := ""
	for _, arg := range args {
		cmdstr += " " + arg
	}

	fmt.Println("command: ./run.sh", cmdstr)

	cmdstr = fmt.Sprintf("cd %s; %s %s", scriptDir, ob.ScriptPath, cmdstr)
	cmd := exec.Command("sh", "-c", cmdstr)
	out, err := cmd.Output()
	fmt.Println(string(out))
	if err != nil {
		return err
	}
	return nil
}
