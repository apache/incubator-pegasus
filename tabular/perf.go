package tabular

import (
	"fmt"
	"io"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/pegasus-kv/collector/aggregate"
	"gopkg.in/yaml.v2"
)

var tableStatsTemplate string = `--- 
Memory: 
  rdb_index_and_filter_blocks_mem_usage: 
    unit: size
  rdb_memtable_mem_usage: 
    unit: size
Peformance: 
  recent_abnormal_count: ~
  recent_expire_count: ~
  recent_filter_count: ~
Read: 
  get_qps: ~
  multi_get_qps: ~
  read_bytes: 
    unit: size
  scan_qps: ~
Storage: 
  rdb_estimate_num_keys: ~
  sst_storage_mb: ~
Write: 
  check_and_mutate_qps: ~
  check_and_set_qps: ~
  incr_qps: ~
  multi_put_qps: ~
  put_qps: ~
  write_bytes: 
    unit: size
`

// The default statFormatter if no unit is specified
func defaultStatFormatter(v float64) string {
	return humanize.SI(v, "")
}

// Used for counter with `"unit" : "size"`.
func sizeStatFormatter(v float64) string {
	return humanize.Bytes(uint64(v))
}

type statFormatter func(float64) string

// PrintTableStatsTabular prints table stats in a number of sections,
// according to the predefined template.
func PrintTableStatsTabular(writer io.Writer, tables map[int32]*aggregate.TableStats) {
	var sections map[string]interface{}
	yaml.Unmarshal([]byte(tableStatsTemplate), &sections)

	for sect, columns := range sections {
		// print section
		table := tablewriter.NewWriter(writer)
		table.SetBorders(tablewriter.Border{Left: false, Right: false, Top: true, Bottom: false})
		table.SetRowSeparator("=")
		table.SetHeader([]string{sect})
		table.Render()

		header := append([]string{"AppID", "Name", "Partitions"})
		var formatters []statFormatter
		for columnName, attrs := range columns.(map[interface{}]interface{}) {
			header = append(header, columnName.(string))

			if attrs == nil {
				formatters = append(formatters, defaultStatFormatter)
			} else {
				attrsMap := attrs.(map[interface{}]interface{})
				unit := attrsMap["unit"]
				if unit == "size" {
					formatters = append(formatters, sizeStatFormatter)
				} else {
					panic(fmt.Sprintf("invalid unit %s in template", unit))
				}
			}
		}

		tabWriter := tablewriter.NewWriter(writer)
		tabWriter.SetBorder(false)
		tabWriter.SetHeader(header)
		for _, tb := range tables {
			// each table displays as a row
			var row []string
			row = append(row, fmt.Sprintf("%d", tb.AppID), tb.TableName, fmt.Sprintf("%d", len(tb.Partitions)))
			for i, key := range header[3:] {
				row = append(row, formatters[i](tb.Stats[key]))
			}
			tabWriter.Append(row)
		}
		tabWriter.Render()
		fmt.Fprintln(writer)
	}
}
