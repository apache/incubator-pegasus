package webui

import (
	"github.com/kataras/iris/v12"
	"github.com/pegasus-kv/collector/client"
	"github.com/spf13/viper"
)

func tablesHandler(ctx iris.Context) {
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

	ctx.View("tables.html")
}
