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

package tabular

import (
	"fmt"
	"io"
	"reflect"

	"github.com/olekukonko/tablewriter"
)

// New creates a tablewriter.Table and allows customizing the table.
//
// Each element should be a simple struct (not pointer) with a number of fields.
// Each field corresponds to a column in the table, the field must have json tag.
// The tag name is the column name in the table header.
//
// For example:
// ```
//
//	type tableStruct struct {
//	  PartitionCount int    `json:"partition_count"`
//	  TableName      string `json:"name"`
//	}
//	var tables []tableStruct
//	...
//	tabular.Print(tables)
//
// ```
func New(writer io.Writer, valueList []interface{}, configurer func(*tablewriter.Table)) *tablewriter.Table {
	header := getHeaderFromValueList(valueList)
	tabWriter := NewTabWriter(writer, header)

	// could replace the default settings
	if configurer != nil {
		configurer(tabWriter)
	}

	for _, val := range valueList {
		// each value displays as a row

		var row []string
		reflectedValue := reflect.ValueOf(val)
		for i := 0; i < reflectedValue.NumField(); i++ {
			// columns are printed in the field order.
			col := fmt.Sprintf("%v", reflectedValue.Field(i).Interface())
			row = append(row, col)
		}
		tabWriter.Append(row)
	}

	return tabWriter
}

func NewTabWriter(writer io.Writer, header []string) *tablewriter.Table {
	tabWriter := tablewriter.NewWriter(writer)
	tabWriter.SetAlignment(tablewriter.ALIGN_LEFT)
	tabWriter.SetAutoFormatHeaders(false)
	tabWriter.SetHeader(header)
	var headerColors []tablewriter.Colors
	for range header {
		headerColors = append(headerColors, tablewriter.Colors{tablewriter.Bold})
	}
	tabWriter.SetHeaderColor(headerColors...)
	return tabWriter
}

// Print out the list of elements in tabular form.
func Print(writer io.Writer, valueList []interface{}) {
	New(writer, valueList, nil).Render()
}

func getHeaderFromValueList(valueList []interface{}) []string {
	if len(valueList) == 0 {
		return []string{"result"}
	}

	var header []string
	val := valueList[0]
	reflectedType := reflect.TypeOf(val)
	for i := 0; i < reflectedType.NumField(); i++ {
		// field tag
		jsonTagName := reflectedType.Field(i).Tag.Get("json")
		header = append(header, jsonTagName)
	}

	return header
}
