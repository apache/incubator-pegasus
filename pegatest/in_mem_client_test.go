// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegatest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFakeClient_SetAndGet(t *testing.T) {
	c := NewClientInMemory()

	c.Set(context.Background(), "temp", []byte("h1"), []byte("s1"), []byte("v1"))
	c.Set(context.Background(), "temp", []byte("h2"), []byte("s2"), []byte("v2"))

	value, _ := c.Get(context.Background(), "temp", []byte("h1"), []byte("s1"))
	assert.Equal(t, []byte("v1"), value)

	value, _ = c.Get(context.Background(), "temp", []byte("h2"), []byte("s2"))
	assert.Equal(t, []byte("v2"), value)
}

func TestFakeClient_DelAndGet(t *testing.T) {
	c := NewClientInMemory()

	c.Set(context.Background(), "temp", []byte("h1"), []byte("s1"), []byte("v1"))

	_ = c.Del(context.Background(), "temp", []byte("h1"), []byte("s1"))

	value, _ := c.Get(context.Background(), "temp", []byte("h1"), []byte("s1"))
	assert.Nil(t, value)
}
