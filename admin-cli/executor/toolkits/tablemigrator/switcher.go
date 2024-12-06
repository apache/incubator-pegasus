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

package tablemigrator

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
	"github.com/go-zookeeper/zk"
)

func SwitchMetaAddrs(client *executor.Client, zkAddr string, zkRoot string, tableName string, targetAddrs string) error {
	cluster, err := client.Meta.QueryClusterInfo()
	if err != nil {
		return err
	}

	if zkAddr == "" {
		zkAddr = cluster["zookeeper_hosts"]
	}
	zkConn, _, err := zk.Connect([]string{zkAddr}, time.Duration(1000*1000*1000))
	if err != nil {
		return err
	}
	defer zkConn.Close()

	currentRemoteZKInfo, err := ReadZkData(zkConn, zkRoot, tableName)
	if err != nil {
		return err
	}

	currentLocalCluster := cluster["cluster_name"]
	if currentRemoteZKInfo.ClusterName != currentLocalCluster {
		return fmt.Errorf("current remote table is not `current local cluster`, remote vs expect= %s : %s",
			currentRemoteZKInfo.ClusterName, currentLocalCluster)
	}

	originMeta := client.Meta
	targetAddrList := strings.Split(targetAddrs, ",")
	pegasusClient, _ := executor.NewClient(os.Stdout, targetAddrList, true)
	targetMeta := pegasusClient.Meta
	env := map[string]string{
		"replica.deny_client_request": "reconfig*all",
	}

	targetCluster, err := targetMeta.QueryClusterInfo()
	if err != nil {
		return err
	}
	_, updatedZkInfo, err := WriteZkData(zkConn, zkRoot, tableName, targetCluster["cluster_name"], targetAddrs)
	if err != nil {
		return err
	}

	err = originMeta.UpdateAppEnvs(tableName, env)
	if err != nil {
		return err
	}
	toolkits.LogInfo(fmt.Sprintf("%s has updated metaproxy addr from %v to %v, current table env is %v\n", tableName, currentRemoteZKInfo, updatedZkInfo, env))
	return nil
}

type MetaProxyTable struct {
	ClusterName string `json:"cluster_name"`
	MetaAddrs   string `json:"meta_addrs"`
}

func ReadZkData(zkConn *zk.Conn, root string, table string) (*MetaProxyTable, error) {
	tablePath := fmt.Sprintf("%s/%s", root, table)
	exist, _, _ := zkConn.Exists(tablePath)
	if !exist {
		return nil, fmt.Errorf("can't find the zk path: %s", tablePath)
	}

	data, _, err := zkConn.Get(tablePath)
	if err != nil {
		return nil, err
	}

	metaProxyTable := MetaProxyTable{}
	err = json.Unmarshal(data, &metaProxyTable)
	if err != nil {
		return nil, err
	}
	return &metaProxyTable, nil
}

func WriteZkData(zkConn *zk.Conn, root string, table string, cluster string, addrs string) (string, string, error) {
	zkData := encodeToZkNodeData(cluster, addrs)
	tablePath := fmt.Sprintf("%s/%s", root, table)
	exist, stat, _ := zkConn.Exists(tablePath)
	if !exist {
		_, err := zkConn.Create(tablePath, zkData, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return "", "", err
		}
	}
	_, err := zkConn.Set(tablePath, zkData, stat.Version)
	if err != nil {
		return "", "", err
	}

	return tablePath, string(zkData), nil
}

func encodeToZkNodeData(cluster string, addr string) []byte {
	data := fmt.Sprintf("{\"cluster_name\": \"%s\", \"meta_addrs\": \"%s\"}", cluster, addr)
	return []byte(data)
}
