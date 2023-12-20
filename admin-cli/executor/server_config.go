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
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/go-client/session"
)

// map[*util.PegasusNode]*util.Result is not sorted, pass nodes is for print sorted result
type printResponse func(nodeType session.NodeType, sortedNodeList []string, resp map[string]*util.Result)

type action struct {
	request util.HTTPRequestFunc
	print   printResponse
}

var actionsMap = map[string]action{
	"list": {listConfig, printConfigList},
	"get":  {getConfig, printConfigValue},
	"set":  {updateConfig, printConfigUpdate},
}

var sectionsMap = map[session.NodeType]string{
	session.NodeTypeMeta:    "meta_server,security",
	session.NodeTypeReplica: "pegasus.server,security,replication,block_service,nfs,task",
	// TODO(jiashuo1) support collector
}

type response struct {
	Name    string
	Section string
	Tags    string
	Value   string
}

// TODO(jiashuo1) not support update collector config
func ConfigCommand(client *Client, nodeType session.NodeType, nodeAddr string, name string, actionType string, value string) error {
	var nodes []*util.PegasusNode
	if len(nodeAddr) == 0 {
		// send http-commands to all nodeType nodes
		nodes = client.Nodes.GetAllNodes(nodeType)
	} else {
		n, err := client.Nodes.GetNode(nodeAddr, nodeType)
		if err != nil {
			return err
		}
		nodes = append(nodes, n)
	}

	if ac, ok := actionsMap[actionType]; ok {
		cmd := util.Arguments{
			Name:  name,
			Value: value,
		}
		results := util.BatchCallHTTP(nodes, ac.request, cmd)

		var sortedNodeList []string
		for _, n := range nodes {
			sortedNodeList = append(sortedNodeList, n.CombinedAddr())
		}
		sort.Strings(sortedNodeList)
		ac.print(nodeType, sortedNodeList, results)
	} else {
		return fmt.Errorf("invalid request type: %s", actionType)
	}

	return nil
}

func listConfig(addr string, cmd util.Arguments) (string, error) {
	url := fmt.Sprintf("http://%s/configs", addr)
	return util.CallHTTPGet(url)
}

func printConfigList(nodeType session.NodeType, sortedNodeList []string, results map[string]*util.Result) {
	fmt.Printf("CMD: list \n")
	for _, node := range sortedNodeList {
		cmdRes := results[node]
		if cmdRes.Err != nil {
			fmt.Printf("[%s] %s\n", node, cmdRes.Err)
			continue
		}

		var respMap map[string]response
		err := json.Unmarshal([]byte(cmdRes.Resp), &respMap)
		if err != nil {
			fmt.Printf("[%s] %s\n", node, err)
			continue
		}

		fmt.Printf("[%s]\n", node)
		var resultSorted []string
		for _, value := range respMap {
			if value.Tags == "flag_tag::FT_MUTABLE" && strings.Contains(sectionsMap[nodeType], value.Section) {
				resultSorted = append(resultSorted, fmt.Sprintf("\t[%s] %s=%s\n", value.Section, value.Name, value.Value))
			}
		}
		sort.Strings(resultSorted)
		resStr := fmt.Sprintf("%s", resultSorted)
		// resStr is like [...], print need delete the `[` and `]` character
		fmt.Print(resStr[1 : len(resStr)-1])
	}
}

func getConfig(addr string, cmd util.Arguments) (string, error) {
	url := fmt.Sprintf("http://%s/config?name=%s", addr, cmd.Name)
	return util.CallHTTPGet(url)
}

func printConfigValue(nodeType session.NodeType, sortedNodeList []string, results map[string]*util.Result) {
	fmt.Printf("CMD: get \n")
	for _, node := range sortedNodeList {
		cmdRes := results[node]
		if cmdRes.Err != nil {
			fmt.Printf("[%s] %s\n", node, cmdRes.Err)
			continue
		}

		var resp response
		err := json.Unmarshal([]byte(cmdRes.Resp), &resp)
		if err != nil {
			fmt.Printf("[%s] %s\n", node, cmdRes.Resp)
			continue
		}

		if !strings.Contains(sectionsMap[nodeType], resp.Section) {
			fmt.Printf("[%s] %s is not config on %s\n", node, resp.Name, nodeType)
			continue
		}

		fmt.Printf("[%s] %s=%s\n", node, resp.Name, resp.Value)
	}
}

func updateConfig(addr string, cmd util.Arguments) (string, error) {
	url := fmt.Sprintf("http://%s/updateConfig?%s=%s", addr, cmd.Name, cmd.Value)
	return util.CallHTTPGet(url)
}

func printConfigUpdate(nodeType session.NodeType, sortedNodeList []string, results map[string]*util.Result) {
	fmt.Printf("CMD: set \n")
	for _, node := range sortedNodeList {
		cmdRes := results[node]
		if cmdRes.Err != nil {
			fmt.Printf("[%s] %s\n", node, cmdRes.Err)
			continue
		}

		var resMap map[string]string
		err := json.Unmarshal([]byte(cmdRes.Resp), &resMap)
		if err != nil {
			fmt.Printf("[%s] %s\n", node, err)
			continue
		}

		fmt.Printf("[%s] %s\n", node, resMap["update_status"])
	}
}
