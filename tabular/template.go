package tabular

import (
	"fmt"
	"io"

	"github.com/dustin/go-humanize"
	"gopkg.in/yaml.v2"
)

// Template receives a yaml template for printing the tabular contents.
// A template must contain one or more sections, each is displayed as
// a table. A sections could contain multiple keys, each represents as
// the table column. Template iterates all the items in every section,
// one item acts as a row in the table.
type Template struct {
	// The sections are in the top-down order as they are declared in the template.
	sections []*section

	colValFunc ColumnValueFunc

	commonColFunc  CommonColumnsFunc
	commonColNames []string
}

// NewTemplate parses the given template.
func NewTemplate(template string) *Template {
	// use MapSlice to preserve the order.
	var sections yaml.MapSlice
	err := yaml.Unmarshal([]byte(template), &sections)
	if err != nil {
		panic(err)
	}

	t := &Template{}
	for _, kv := range sections {
		sec := &section{name: fmt.Sprint(kv.Key)}
		columns := kv.Value.(yaml.MapSlice)
		for _, col := range columns {
			colAttrs := &ColumnAttributes{Name: fmt.Sprint(col.Key), Attrs: make(map[string]string)}
			if col.Value != nil {
				attrs := col.Value.(yaml.MapSlice)
				for _, attr := range attrs {
					name := fmt.Sprint(attr.Key)
					value := fmt.Sprint(attr.Value)
					colAttrs.Attrs[name] = value
				}
			}
			colAttrs.formatter = getFormatter(colAttrs.Attrs)
			sec.columns = append(sec.columns, colAttrs)
		}

		t.sections = append(t.sections, sec)
	}
	return t
}

func getFormatter(attrs map[string]string) columnValueFormatter {
	t, ok := attrs["unit"]
	if !ok {
		return defaultFormatter
	}
	switch t {
	case "byte":
		return byteStatFormatter
	case "MB":
		return megabyteStatFormatter
	default:
		panic(fmt.Sprintf("unexpected unit %vs", t))
	}
}

// ColumnValueFunc takes the column value from a user record.
type ColumnValueFunc func(col *ColumnAttributes, rowData interface{}) interface{}

// SetColumnValueFunc configures ColumnValueFunc
func (t *Template) SetColumnValueFunc(f ColumnValueFunc) {
	t.colValFunc = f
}

// CommonColumnsFunc returns a list of common columns.
type CommonColumnsFunc func(rowData interface{}) []string

// SetCommonColumns sets the columns that are exactly the same among sections.
// This is a conveninence util to prevent repeatedly declaring the column for each section.
func (t *Template) SetCommonColumns(columnNames []string, f CommonColumnsFunc) {
	t.commonColNames = columnNames
	t.commonColFunc = f
}

// Render template output
func (t *Template) Render(writer io.Writer, rows []interface{}) {
	for _, sect := range t.sections {
		// print section
		fmt.Fprintf(writer, "[%s]\n", sect.name)

		tabWriter := NewTabWriter(writer)
		header := t.commonColNames
		for _, col := range sect.columns {
			header = append(header, col.Name)
		}
		tabWriter.SetHeader(header)
		for _, row := range rows {
			rowColumns := t.commonColFunc(row)
			for _, col := range sect.columns {
				columnValue := t.colValFunc(col, row)
				rowColumns = append(rowColumns, col.formatter(columnValue))
			}
			tabWriter.Append(rowColumns)
		}
		tabWriter.Render()
	}
}

// section display as a table.
type section struct {
	name    string
	columns []*ColumnAttributes
}

// ColumnAttributes is the
type ColumnAttributes struct {
	Name string

	Attrs map[string]string

	// The formatter is optionally declared in `unit`
	// If `unit` is "byte", byteStatFormatter is used.
	// If `unit` is "MB", megabyteStatFormatter is used.
	// Otherwise defaultFormatter is used.
	formatter columnValueFormatter
}

// The default if no unit is specified.
func defaultFormatter(v interface{}) string {
	if fv, ok := v.(float64); ok {
		return humanize.SIWithDigits(fv, 2, "")
	}
	return fmt.Sprintf("%v", v)
}

// Used for `"unit" : "byte"`.
func byteStatFormatter(v interface{}) string {
	fv, ok := v.(float64)
	if !ok {
		panic("data to unit:\"byte\" must be float64")
	}
	return humanize.Bytes(uint64(fv))
}

// Used for `"unit" : "MB"`.
func megabyteStatFormatter(v interface{}) string {
	fv, ok := v.(float64)
	if !ok {
		panic("data to unit:\"MB\" must be float64")
	}
	return humanize.Bytes(uint64(fv) << 20)
}

type columnValueFormatter func(interface{}) string
