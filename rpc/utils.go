// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package rpc

import (
	"net"
)

// err requires to be non-nil
func IsNetworkTimeoutErr(err error) bool {
	// if it's a network timeout error
	opErr, ok := err.(*net.OpError)
	if ok {
		return opErr.Timeout()
	}

	return false
}
