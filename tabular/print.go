package tabular

import (
	"encoding/json"
	"io"

	jsoniter "github.com/json-iterator/go"
	"github.com/olekukonko/tablewriter"
)

// Print out the list of elements in tabular form.
func Print(writer io.Writer, valueList []interface{}, header []string) {
	tabWriter := tablewriter.NewWriter(writer)
	tabWriter.SetHeader(header)
	var headerColors []tablewriter.Colors
	for range header {
		headerColors = append(headerColors, tablewriter.Colors{tablewriter.Bold})
	}
	tabWriter.SetHeaderColor(headerColors...)

	for _, val := range valueList {
		outputBytes, _ := json.Marshal(val)
		jsonObj := jsoniter.Get(outputBytes)

		var row []string
		for _, key := range jsonObj.Keys() {
			row = append(row, jsonObj.Get(key).ToString())
		}
		tabWriter.Append(row)
	}

	tabWriter.Render()
}
