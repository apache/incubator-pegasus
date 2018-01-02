// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package rpc

import (
	"io"
	"net"
)

// err requires to be non-nil
func IsRetryableError(err error) bool {
	opErr, ok := err.(*net.OpError)
	if ok {
		return opErr.Timeout()
	}

	// if it's not ready, retry until the connection established
	if err == ErrConnectionNotReady {
		return true
	}

	return err == io.EOF
}
