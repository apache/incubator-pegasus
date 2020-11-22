package tabular

import (
	"fmt"
	"io"

	jsoniter "github.com/json-iterator/go"
	"github.com/olekukonko/tablewriter"
	"github.com/pegasus-kv/collector/aggregate"
)

var tableStatsTemplate string = `{
	"Write": [
		"put_qps",
		"multi_put_qps",
		"incr_qps",
		"check_and_set_qps",
		"check_and_mutate_qps",
		"write_bytes"
	],
	"Read": [
		"get_qps",
		"multi_get_qps",
		"scan_qps",
		"read_bytes"
	],
	"Storage": [
		"sst_storage_mb",
		"rdb_estimate_num_keys"
	],
	"Memory": [
		"rdb_memtable_mem_usage",
		"rdb_index_and_filter_blocks_mem_usage"
	],
	"Peformance": [
		"recent_expire_count",
		"recent_filter_count",
		"recent_abnormal_count"
	]
}`

// PrintTableStatsTabular prints table stats in a number of sections,
// according to the predefined template.
func PrintTableStatsTabular(writer io.Writer, tables map[int32]*aggregate.TableStats) {
	iter := jsoniter.ParseString(jsoniter.ConfigDefault, tableStatsTemplate)
	iter.ReadMapCB(func(i *jsoniter.Iterator, section string) bool {
		// print section
		table := tablewriter.NewWriter(writer)
		table.SetBorders(tablewriter.Border{Left: false, Right: false, Top: true, Bottom: false})
		table.SetRowSeparator("=")
		table.SetHeader([]string{section})
		table.Render()

		// print table
		header := append([]string{"AppID", "Name", "Partitions"})
		i.ReadArrayCB(func(i *jsoniter.Iterator) bool {
			header = append(header, i.ReadString())
			return true
		})
		tabWriter := tablewriter.NewWriter(writer)
		tabWriter.SetBorder(false)
		tabWriter.SetHeader(header)
		for _, tb := range tables {
			// each table displays as a row
			var row []string
			row = append(row, fmt.Sprintf("%d", tb.AppID), tb.TableName, fmt.Sprintf("%d", len(tb.Partitions)))
			for _, key := range header[3:] {
				row = append(row, fmt.Sprintf("%v", tb.Stats[key]))
			}
			tabWriter.Append(row)
		}
		tabWriter.Render()
		fmt.Fprintln(writer)
		return true
	})
}
