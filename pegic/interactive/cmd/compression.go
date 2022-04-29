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

package cmd

import (
	"fmt"
	"strings"

	"github.com/apache/incubator-pegasus/pegic/executor/util"
	"github.com/apache/incubator-pegasus/pegic/interactive"
	"github.com/desertbit/grumble"
)

func init() {
	rootCmd := &grumble.Command{
		Name: "compression",
		Help: "read the current compression algorithm",
		Run: func(c *grumble.Context) error {
			if globalContext.Compressor == nil {
				c.App.Println("No compression is used")
			} else {
				c.App.Printf("Compression: %s\n", globalContext.Compressor.String())
			}
			return nil
		},
	}

	rootCmd.AddCommand(&grumble.Command{
		Name:    "set",
		Aliases: []string{"SET"},
		Help:    "reset the compression algorithm, default no",
		Run: func(c *grumble.Context) error {
			algo := strings.TrimSpace(c.Args.String("ALGORITHM"))
			if algo == "" || algo == "no" {
				c.App.Println("Reset to no compression mode")
				globalContext.Compressor = nil
				return nil
			}
			globalContext.Compressor = util.NewCompression(algo)
			if globalContext.Compressor == nil {
				return fmt.Errorf("Invalid compression algorithm \"%s\"", algo)
			}
			c.App.Println("ok")
			return nil
		},
		Args: func(a *grumble.Args) {
			a.String("ALGORITHM", "Compression algorithm. By default no compression is used. Options: zstd", grumble.Default(""))
		},
		Completer: compressionCompleter,
	})

	interactive.App.AddCommand(rootCmd)
}

func compressionCompleter(prefix string, args []string) []string {
	return filterStringWithPrefix([]string{
		"zstd",
		"no",
	}, prefix)
}
