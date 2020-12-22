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
	"os"
	"pegic/interactive"
	"pegic/interactive/cmd"
	"strings"

	"github.com/spf13/cobra"
)

// Root command for pegic.
var Root *cobra.Command

func init() {
	var metaList *string

	Root = &cobra.Command{
		Use:   "pegic [--meta|-m <meta-list>]",
		Short: "pegic: Pegasus Interactive Command-Line tool",
		PreRun: func(c *cobra.Command, args []string) {
			metaAddrs := strings.Split(*metaList, ",")
			err := cmd.Init(metaAddrs)
			if err != nil {
				c.PrintErrln(err)
				os.Exit(1)
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			// the default entrance is interactive
			interactive.Run()
		},
	}

	metaList = Root.Flags().StringP("meta", "m", "127.0.0.1:34601,127.0.0.1:34602", "the list of MetaServer addresses")
}
