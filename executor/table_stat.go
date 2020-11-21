package executor

import (
	"admin-cli/tabular"

	"github.com/pegasus-kv/collector/aggregate"
)

// TableStat command.
func TableStat(c *Client) error {
	ag := aggregate.NewTableStatsAggregator(c.MetaAddresses)
	tableStats, _ := ag.Aggregate()
	// TODO(wutao): limit table count, if table count exceeds a number, the result
	// can be written to a file or somewhere instead.

	tabular.PrintTableStatsTabular(c, tableStats)
	return nil
}
