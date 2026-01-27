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

// MetricClient encapsulates the APIs that are used to pull metrics from target node.
type MetricClient interface {
	// GetBriefValueSnapshot pulls the metrics
	GetBriefValueSnapshot(ctx context.Context, filter MetricBriefValueFilter) (*MetricQueryBriefValueSnapshot, error)
}

type MetricClientConfig struct {
	Host string // The host of the target node
	Port uint16 // The port of the target node
}

func NewMetricClient(cfg *MetricClientConfig) MetricClient {
	return &metricClientImpl{
		host:   net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))),
		client: &http.Client{},
	}
}

type metricClientImpl struct {
	host   string // The address(<HOST>:<PORT>) of the target node
	client *http.Client
}

func (c *metricClientImpl) GetBriefValueSnapshot(ctx context.Context, filter MetricBriefValueFilter) (*MetricQueryBriefValueSnapshot, error) {
	u := url.URL{
		Scheme:   "http",
		Host:     c.host,
		Path:     MetricQueryPath,
		RawQuery: filter.ToQueryFields().Encode(), // Generate query string
	}

	// Create the http request to the target node.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	// Send the http request and return the response.
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	// Fail the request if the status is not OK.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status: %s", resp.Status)
	}

	// Decode the response as the object.
	var result MetricQueryBriefValueSnapshot
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, err
}
