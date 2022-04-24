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

package main

import (
	"fmt"
	"net"
	"strings"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/desertbit/grumble"
	"github.com/pegasus-kv/admin-cli/admin-cli/cmd"
	"github.com/pegasus-kv/admin-cli/admin-cli/shell"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	// pegasus-go-client's logs use the same logger as admin-cli.
	pegalog.SetLogger(logrus.StandardLogger())
	// configure log destination
	logrus.SetOutput(&lumberjack.Logger{
		Filename:  "./shell.log",
		LocalTime: true,
	})
	logrus.SetLevel(logrus.DebugLevel)

	shell.App.OnInit(func(a *grumble.App, flags grumble.FlagMap) error {
		metaListStr := flags.String("meta")
		metaList := strings.Split(metaListStr, ",")
		for _, metaIPPort := range metaList {
			_, err := net.ResolveTCPAddr("tcp4", metaIPPort)
			if err != nil {
				return fmt.Errorf("Invalid MetaServer TCP address [%s]", err)
			}
		}
		cmd.Init(metaList)
		return nil
	})
	grumble.Main(shell.App)
}
