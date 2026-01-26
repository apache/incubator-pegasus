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
	"net/url"
	"strings"
)

type MetricBriefValueFilter interface {
	ToQueryFields() url.Values
}

func NewMetricBriefValueFilter(
	entityTypes []string,
	entityIDs []string,
	entityAttributes map[string]string,
	entityMetrics []string,
) MetricBriefValueFilter {
	return &metricFilter{
		WithMetricFields: []string{MetricFieldName, MetricFieldSingleValue},
		EntityTypes:      entityTypes,
		EntityIDs:        entityIDs,
		EntityAttributes: entityAttributes,
		EntityMetrics:    entityMetrics,
	}
}

type metricFilter struct {
	WithMetricFields []string
	EntityTypes      []string
	EntityIDs        []string
	EntityAttributes map[string]string
	EntityMetrics    []string
}

func (filter *metricFilter) ToQueryFields() url.Values {
	fields := make(url.Values)

	fields.Set(MetricQueryWithMetricFields, joinValues(filter.WithMetricFields))
	fields.Set(MetricQueryTypes, joinValues(filter.EntityTypes))
	fields.Set(MetricQueryIDs, joinValues(filter.EntityIDs))

	attrs := make([]string, 0, len(filter.EntityAttributes)*2)
	for attrKey, attrValue := range filter.EntityAttributes {
		attrs = append(attrs, attrKey, attrValue)
	}
	fields.Set(MetricQueryAttributes, joinValues(attrs))

	fields.Set(MetricQueryMetrics, joinValues(filter.EntityMetrics))

	return fields
}

func joinValues(values []string) string {
	return strings.Join(values, ",")
}
