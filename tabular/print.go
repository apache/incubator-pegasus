package tabular

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/olekukonko/tablewriter"
)

// Print out the list of elements in tabular form.
// Each element should be a simple struct (not pointer) with a number of fields.
func Print(writer io.Writer, valueList []interface{}) {
	tabWriter := tablewriter.NewWriter(writer)
	header := getHeaderFromValueList(valueList)
	tabWriter.SetHeader(header)
	var headerColors []tablewriter.Colors
	for range header {
		headerColors = append(headerColors, tablewriter.Colors{tablewriter.Bold})
	}
	tabWriter.SetHeaderColor(headerColors...)

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

	tabWriter.Render()
}

func getHeaderFromValueList(valueList []interface{}) []string {
	var header []string

	val := valueList[0]
	reflectedType := reflect.TypeOf(val)
	for i := 0; i < reflectedType.NumField(); i++ {
		// field tag
		jsonTagName := reflectedType.Field(i).Tag.Get("json")
		header = append(header, formatColumnName(jsonTagName))
	}

	return header
}

func formatColumnName(jsonTagName string) string {
	words := strings.Split(jsonTagName, "_")
	for i, w := range words {
		words[i] = strings.ToTitle(w)
	}
	return strings.Join(words, "\n")
}
