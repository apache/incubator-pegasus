package webui

import (
	"github.com/kataras/iris/v12"
	"github.com/pegasus-kv/collector/aggregate"
	"github.com/pegasus-kv/collector/client"
	"github.com/spf13/viper"
)

var indexPageClusterStats = []string{
	"write_bytes",
	"read_bytes",
	"write_qps",
	"read_qps",
}

func renderIndexClusterCharts(ctx iris.Context) {
	type perfCounterHTML struct {
		PerfCounter string
		Values      []float64
	}
	var PerfCounters []*perfCounterHTML

	snapshots := aggregate.SnapshotClusterStats()
	for _, s := range indexPageClusterStats {
		PerfCounters = append(PerfCounters, &perfCounterHTML{
			PerfCounter: s,
		})
		p := PerfCounters[len(PerfCounters)-1]
		for _, snapshot := range snapshots {
			if v, found := snapshot.Stats[s]; found {
				p.Values = append(p.Values, v)
			}
		}
	}
	ctx.ViewData("PerfCounters", PerfCounters)

	var PerfIDs []string
	for _, sn := range snapshots {
		PerfIDs = append(PerfIDs, sn.Timestamp.Format("15:04:00"))
	}
	ctx.ViewData("PerfIDs", PerfIDs)
}

func indexHandler(ctx iris.Context) {
	renderIndexClusterCharts(ctx)

	metaClient := client.NewMetaClient(viper.GetString("meta_server"))
	tables, err := metaClient.ListTables()
	if err != nil {
		ctx.ResponseWriter().WriteString("Failed to list tables from MetaServer")
		ctx.StatusCode(iris.StatusInternalServerError)
		return
	}
	type tableHTMLRow struct {
		TableName string
		Link      string
	}
	var Tables []tableHTMLRow
	for _, tb := range tables {
		Tables = append(Tables, tableHTMLRow{TableName: tb.TableName})
	}
	ctx.ViewData("Tables", Tables)

	ctx.View("index.html")
}
