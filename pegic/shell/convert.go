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

package shell

import (
	"github.com/apache/incubator-pegasus/pegic/executor/util"
	"github.com/spf13/cobra"
)

func init() {
	// flags
	var from *string
	var to *string

	cmd := &cobra.Command{
		Use:   "convert",
		Short: "convert bytes from one encoding to the other",
		Args:  cobra.ExactArgs(1),
		Run: func(c *cobra.Command, args []string) {
			fromEnc := util.NewEncoder(*from)
			if fromEnc == nil {
				c.PrintErrf("invalid encoding: \"%s\"", *from)
				return
			}
			toEnc := util.NewEncoder(*to)
			if toEnc == nil {
				c.PrintErrf("invalid encoding: \"%s\"", *to)
				return
			}

			data, err := fromEnc.EncodeAll(args[0])
			if err != nil {
				c.PrintErr(err)
				return
			}
			result, err := toEnc.DecodeAll(data)
			if err != nil {
				c.PrintErr(err)
				return
			}
			c.Println(result)
		},
	}

	from = cmd.Flags().String("from", "", "the original encoding of the given bytes")
	to = cmd.Flags().String("to", "", "the original encoding of the given bytes")
	_ = cmd.MarkFlagRequired("from")
	_ = cmd.MarkFlagRequired("to")

	Root.AddCommand(cmd)
}
