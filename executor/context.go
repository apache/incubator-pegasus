/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package executor

import (
	"fmt"
	"io"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/pegic/executor/util"
)

type Context struct {
	// Every command should use Context as the fmt.Fprint's writer.
	io.Writer

	pegasus.Client

	Meta *session.MetaManager

	// default to nil
	UseTable     pegasus.TableConnector
	UseTableName string
	// default to 0
	UseTablePartitionCount int

	// default to nil
	Compressor util.BytesCompression

	HashKeyEnc, SortKeyEnc, ValueEnc util.Encoder
}

func NewContext(writer io.Writer, metaAddrs []string) *Context {
	c := &Context{
		Writer: writer,
		Client: pegasus.NewClient(pegasus.Config{
			MetaServers: metaAddrs,
		}),
		Meta: session.NewMetaManager(metaAddrs, session.NewNodeSession),

		// by default, string uses utf-8 as encoding to bytes
		HashKeyEnc: util.NewEncoder("utf8"),
		SortKeyEnc: util.NewEncoder("utf8"),
		ValueEnc:   util.NewEncoder("utf8"),
	}
	return c
}

// String returns a human-readable string of context.
func (c *Context) String() string {
	output := "\nEncoding:\n"
	output += "  - HashKey: " + c.HashKeyEnc.String() + "\n"
	output += "  - SortKey: " + c.SortKeyEnc.String() + "\n"
	output += "  - Value: " + c.ValueEnc.String() + "\n"
	if c.Compressor != nil {
		output += "Compression:\n"
		output += "  " + c.Compressor.String() + "\n"
	}
	return output
}

// readPegasusArgs assumes the args to be [hashkey, sortkey, value].
// It returns the bytes encoded from the args if no failure.
func readPegasusArgs(ctx *Context, args []string) ([][]byte, error) {
	// the first argument must be hashkey
	hashkey, err := ctx.HashKeyEnc.EncodeAll(args[0])
	if err != nil {
		return nil, err
	}
	if len(args) == 1 {
		return [][]byte{hashkey}, nil
	}

	sortkey, err := ctx.SortKeyEnc.EncodeAll(args[1])
	if err != nil {
		return nil, err
	}
	if len(args) == 2 {
		return [][]byte{hashkey, sortkey}, nil
	}

	value, err := ctx.ValueEnc.EncodeAll(args[2])
	if err != nil {
		return nil, err
	}
	if len(args) == 3 {
		return [][]byte{hashkey, sortkey, value}, nil
	}

	panic("more than 3 arguments are given")
}

// printPegasusRecord prints bytes from pegasus in pretty and parsable form.
func printPegasusRecord(ctx *Context, hashKey, sortKey, value []byte) error {
	if ctx.Compressor != nil {
		var err error
		value, err = ctx.Compressor.Decompress(value)
		if err != nil {
			return fmt.Errorf("%s failed to decompress value: %s", ctx.Compressor, err)
		}
	}

	hashKeyStr, err := ctx.HashKeyEnc.DecodeAll(hashKey)
	if err != nil {
		return fmt.Errorf("HashKey can not be decoded in %s: %s", ctx.HashKeyEnc, err)
	}
	sortkeyStr, err := ctx.SortKeyEnc.DecodeAll(sortKey)
	if err != nil {
		return fmt.Errorf("SortKey can not be decoded in %s: %s", ctx.SortKeyEnc, err)
	}
	valueStr, err := ctx.ValueEnc.DecodeAll(value)
	if err != nil {
		bytesEnc := util.NewEncoder("javabytes")
		valueStr, _ := bytesEnc.DecodeAll(value)
		return fmt.Errorf("Value can not be decoded in %s: %s [SortKey: \"%s\"]\nValue: [JAVABYTES]\n%s", ctx.ValueEnc, err, sortKey, valueStr)
	}
	fmt.Fprintf(ctx, "%s : %s : %s\n", hashKeyStr, sortkeyStr, valueStr)
	return nil
}
