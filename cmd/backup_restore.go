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

	"github.com/desertbit/grumble"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/shell"
)

func init() {

	shell.AddCommand(&grumble.Command{
		Name:  "backup",
		Help:  "backup a table",
		Usage: "backup <TABLE_ID> <PROVIDER_TYPE> [SPECIFIC_BACKUP_PATH]",

		Args: func(a *grumble.Args) {
			a.Int("tableID", "the table ID")
			a.String("providerType", "the provider type of backup")
			a.String("backupPath", "the user specified backup path", grumble.Default(""))
		},
		Run: func(c *grumble.Context) error {
			tableID := c.Args.Int("tableID")
			providerType := c.Args.String("providerType")
			backupPath := c.Args.String("backupPath")
			return executor.BackupTable(pegasusClient, tableID, providerType, backupPath)
		},
	})

	shell.AddCommand(&grumble.Command{
		Name:  "query-backup-status",
		Help:  "query backup status",
		Usage: "query-backup-status <TABLE_ID> [BACKUP_ID]",
		Args: func(a *grumble.Args) {
			a.Int("tableID", "the table ID")
			a.Int64("backupID", "the backup ID", grumble.Default(int64(0)))
		},
		Run: func(c *grumble.Context) error {
			tableID := c.Args.Int("tableID")
			backupID := c.Args.Int64("backupID")
			return executor.QueryBackupStatus(pegasusClient, tableID, backupID)
		},
	})

	shell.AddCommand(&grumble.Command{
		Name: "restore",
		Help: "restore a table",
		Usage: `restore 
		<-c|--oldClusterName OLD_CLUSTER_NAME> 
		<-a|--oldTableName OLD_TABLE_NAME> 
		<-i|--oldTableID OLD_TABLE_ID>
		<-t|--timestamp TIMESTAMP/BACKUP_ID>
		<-b|--providerType PROVIDER_TYPE>
		[-n|--newTableName NEW_TABLE_NAME]
		[-r|--restorePath SPECIFIC_RESTORE_PATH]
		[-s|--skipBadPartition SKIP_BAD_PARTITION]
		[-p|--policyName POLICY_NAME]`,
		Run: func(c *grumble.Context) error {
			if c.Flags.String("oldClusterName") == "" {
				return fmt.Errorf("oldClusterName cannot be empty")
			}
			if c.Flags.String("oldTableName") == "" {
				return fmt.Errorf("oldTableName cannot be empty")
			}
			if c.Flags.Int("oldTableID") == 0 {
				return fmt.Errorf("oldTableID cannot be empty")
			}
			if c.Flags.Int64("timestamp") == 0 {
				return fmt.Errorf("timestamp cannot be empty")
			}
			if c.Flags.String("providerType") == "" {
				return fmt.Errorf("providerType cannot be empty")
			}
			var newName string
			if c.Flags.String("newTableName") == "" {
				newName = c.Flags.String("oldTableName")
			} else {
				newName = c.Flags.String("newTableName")
			}
			oldClusterName := c.Flags.String("oldClusterName")
			oldTableName := c.Flags.String("oldTableName")
			oldTableID := c.Flags.Int("oldTableID")
			backupID := c.Flags.Int64("timestamp")
			providerType := c.Flags.String("providerType")
			newTableName := newName
			restorePath := c.Flags.String("restorePath")
			skipBadPartition := c.Flags.Bool("skipBadPartition")
			policyName := c.Flags.String("policyName")
			return executor.RestoreTable(pegasusClient, oldClusterName, oldTableName,
				oldTableID, backupID, providerType, newTableName, restorePath, skipBadPartition, policyName)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("c", "oldClusterName", "", "old_cluster_name, for example, onebox")
			f.String("a", "oldTableName", "", "old_app_name, for example, temp")
			f.Int("i", "oldTableID", 0, "old_app_id, for example, 1")
			f.Int64("t", "timestamp", 0, "timestamp or backup_id")
			f.String("b", "providerType", "", "backup_provider_type, for example, hdfs_zjy")
			f.String("n", "newTableName", "", "new_app_name")
			f.String("r", "restorePath", "", "restore_path")
			f.Bool("s", "skipBadPartition", false, "whether to skip bad partition when create new table")
			f.String("p", "policyName", "", "old_policy_name, only worked for restoring app created before Pegasus2.2.0")
		},
	})
}
