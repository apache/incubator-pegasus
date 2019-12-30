// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus2

import (
	"os"
	"testing"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
)

func TestMain(m *testing.M) {
	pegalog.SetLogger(pegalog.StderrLogger)
	retc := m.Run()
	os.Exit(retc)
}
