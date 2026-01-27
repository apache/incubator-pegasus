// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
)

type MetricClient interface {
	GetBriefValueSnapshot(ctx context.Context, filter MetricBriefValueFilter) (*MetricQueryBriefValueSnapshot, error)
}

type MetricClientConfig struct {
	Host string
	Port uint16
}

func NewMetricClient(cfg *MetricClientConfig) MetricClient {
	return &metricClientImpl{
		client: &http.Client{},
		url: &url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))),
			Path:   "/metrics",
		},
	}
}

type metricClientImpl struct {
	client *http.Client
	url    *url.URL
}

func (c *metricClientImpl) GetBriefValueSnapshot(ctx context.Context, filter MetricBriefValueFilter) (*MetricQueryBriefValueSnapshot, error) {
	c.url.RawQuery = filter.ToQueryFields().Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status: %s", resp.Status)
	}

	var result MetricQueryBriefValueSnapshot
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, err
}
