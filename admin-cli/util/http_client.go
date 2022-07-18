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

package util

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
)

type Arguments struct {
	Name  string
	Value string
}

type Result struct {
	Resp string
	Err  error
}

type HTTPRequestFunc func(addr string, args Arguments) (string, error)

func BatchCallHTTP(nodes []*PegasusNode, request HTTPRequestFunc, args Arguments) map[string]*Result {
	results := make(map[string]*Result)

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, n := range nodes {
		go func(node *PegasusNode) {
			_, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			result, err := request(node.TCPAddr(), args)
			mu.Lock()
			if err != nil {
				results[node.CombinedAddr()] = &Result{Err: err}
			} else {
				results[node.CombinedAddr()] = &Result{Resp: result}
			}
			mu.Unlock()
			wg.Done()
		}(n)
	}
	wg.Wait()

	return results
}

func CallHTTPGet(url string) (string, error) {
	resp, err := resty.New().SetTimeout(time.Second * 10).R().Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to call \"%s\": %s", url, err)
	}
	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("failed to call \"%s\": code=%d", url, resp.StatusCode())
	}
	return string(resp.Body()), nil
}
