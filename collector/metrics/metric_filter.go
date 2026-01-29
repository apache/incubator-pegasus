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

// MetricBriefValueFilter is used to set each field of the query for "Brief Value" (i.e.
// asking for only 2 fields for each metric: "name" and "value"), and generate query string
// of the http request to the target node. Also see *BriefValue* structures in metric_snapshot.go.
type MetricBriefValueFilter interface {
	// ToQueryFields generates the query string according to the required fields.
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

	// Join all the attributes in a map into one string as the value of the attribute field:
	// "key0,val0,key1,val1,key2,val2,..."
	attrs := make([]string, 0, len(filter.EntityAttributes)*2)
	for attrKey, attrValue := range filter.EntityAttributes {
		attrs = append(attrs, attrKey, attrValue)
	}
	fields.Set(MetricQueryAttributes, joinValues(attrs))

	fields.Set(MetricQueryMetrics, joinValues(filter.EntityMetrics))

	return fields
}

// Use comma to join all elements of a slice as the value of a field in a query.
func joinValues(values []string) string {
	return strings.Join(values, ",")
}
