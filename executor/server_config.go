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
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/go-resty/resty/v2"
	"github.com/pegasus-kv/admin-cli/util"
)

type httpRequest func(addr string, cmd command) (string, error)

// map[*util.PegasusNode]*cmdResult is not sorted, pass nodes is for print sorted result
type printResponse func(nodeType session.NodeType, sortedNodeList []string, resp map[string]*cmdResult)

type action struct {
	request httpRequest
	print   printResponse
}

var actionsMap = map[string]action{
	"list": {listConfig, printConfigList},
	"get":  {getConfig, printConfigValue},
	"set":  {updateConfig, printConfigUpdate},
}

var sectionsMap = map[session.NodeType]string{
	session.NodeTypeMeta:    "meta_server,security",
	session.NodeTypeReplica: "pegasus.server,security,replication,block_service,nfs",
	// TODO(jiashuo1) support collector
}

type command struct {
	name  string
	value int64
}

type response struct {
	Name    string
	Section string
	Tags    string
	Value   string
}

type cmdResult struct {
	resp string
	err  error
}

//TODO(jiashuo1) not support update collector config
func ConfigCommand(client *Client, nodeType session.NodeType, nodeAddr string, name string, actionType string, value int64) error {
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
		cmd := command{
			name:  name,
			value: value,
		}
		results := batchCallHTTP(nodes, ac.request, cmd)

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

func batchCallHTTP(nodes []*util.PegasusNode, request httpRequest, cmd command) map[string]*cmdResult {
	results := make(map[string]*cmdResult)

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, n := range nodes {
		go func(node *util.PegasusNode) {
			_, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			result, err := request(node.TCPAddr(), cmd)
			mu.Lock()
			if err != nil {
				results[node.CombinedAddr()] = &cmdResult{err: err}
			} else {
				results[node.CombinedAddr()] = &cmdResult{resp: result}
			}
			mu.Unlock()
			wg.Done()
		}(n)
	}
	wg.Wait()

	return results
}

func callHTTP(url string) (string, error) {
	resp, err := resty.New().SetTimeout(time.Second * 10).R().Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to call \"%s\": %s", url, err)
	}
	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("failed to call \"%s\": code=%d", url, resp.StatusCode())
	}
	return string(resp.Body()), nil
}

func listConfig(addr string, cmd command) (string, error) {
	url := fmt.Sprintf("http://%s/configs", addr)
	return callHTTP(url)
}

func printConfigList(nodeType session.NodeType, sortedNodeList []string, results map[string]*cmdResult) {
	fmt.Printf("CMD: list \n")
	for _, node := range sortedNodeList {
		cmdRes := results[node]
		if cmdRes.err != nil {
			fmt.Printf("[%s] %s\n", node, cmdRes.err)
			continue
		}

		var respMap map[string]response
		err := json.Unmarshal([]byte(cmdRes.resp), &respMap)
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

func getConfig(addr string, cmd command) (string, error) {
	url := fmt.Sprintf("http://%s/config?name=%s", addr, cmd.name)
	return callHTTP(url)
}

func printConfigValue(nodeType session.NodeType, sortedNodeList []string, results map[string]*cmdResult) {
	fmt.Printf("CMD: get \n")
	for _, node := range sortedNodeList {
		cmdRes := results[node]
		if cmdRes.err != nil {
			fmt.Printf("[%s] %s\n", node, cmdRes.err)
			continue
		}

		var resp response
		err := json.Unmarshal([]byte(cmdRes.resp), &resp)
		if err != nil {
			fmt.Printf("[%s] %s\n", node, cmdRes.resp)
			continue
		}

		if !strings.Contains(sectionsMap[nodeType], resp.Section) {
			fmt.Printf("[%s] %s is not config on %s\n", node, resp.Name, nodeType)
			continue
		}

		fmt.Printf("[%s] %s=%s\n", node, resp.Name, resp.Value)
	}
}

func updateConfig(addr string, cmd command) (string, error) {
	url := fmt.Sprintf("http://%s/updateConfig?%s=%d", addr, cmd.name, cmd.value)
	return callHTTP(url)
}

func printConfigUpdate(nodeType session.NodeType, sortedNodeList []string, results map[string]*cmdResult) {
	fmt.Printf("CMD: set \n")
	for _, node := range sortedNodeList {
		cmdRes := results[node]
		if cmdRes.err != nil {
			fmt.Printf("[%s] %s\n", node, cmdRes.err)
			continue
		}

		var resMap map[string]string
		err := json.Unmarshal([]byte(cmdRes.resp), &resMap)
		if err != nil {
			fmt.Printf("[%s] %s\n", node, err)
			continue
		}

		fmt.Printf("[%s] %s\n", node, resMap["update_status"])
	}
}
