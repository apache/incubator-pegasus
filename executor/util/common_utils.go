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
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
)

func Str2Gpid(gpid string) (*base.Gpid, error) {
	splitResult := strings.Split(gpid, ".")
	if len(splitResult) < 2 {
		return &base.Gpid{}, fmt.Errorf("Invalid gpid format [%s]", gpid)
	}

	appId, err := strconv.ParseInt(splitResult[0], 10, 32)

	if err != nil {
		return &base.Gpid{}, fmt.Errorf("Invalid gpid format [%s]", gpid)
	}

	partitionId, err := strconv.ParseInt(splitResult[1], 10, 32)
	if err != nil {
		return &base.Gpid{}, fmt.Errorf("Invalid gpid format [%s]", gpid)
	}

	return &base.Gpid{Appid: int32(appId), PartitionIndex: int32(partitionId)}, nil
}

func SortStructsByField(structs []interface{}, key string) {
	sort.Slice(structs, func(i, j int) bool {
		v1 := reflect.ValueOf(structs[i]).FieldByName(key)
		v2 := reflect.ValueOf(structs[j]).FieldByName(key)
		if v1.Type().Name() == "string" {
			return strings.Compare(v1.String(), v2.String()) < 0
		} else if v1.Type().Name() == "int" {
			return v1.Int() < v2.Int()
		} else {
			panic(fmt.Sprintf("Not support sort %s", v1.Type().Name()))
		}
	})
}

func FormatDate(date int64) string {
	if date != 0 {
		return time.Unix(date, 0).Format("2006-01-02")
	}
	return "unknown"
}
