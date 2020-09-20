package webui

import (
	"github.com/kataras/iris/v12"
	"github.com/pegasus-kv/collector/client"
	"github.com/spf13/viper"
)

func indexHandler(ctx iris.Context) {
	metaClient := client.NewMetaClient(viper.GetString("meta_server"))
	tables, err := metaClient.ListTables()
	if err != nil {
		ctx.StatusCode(iris.StatusInternalServerError)
		return
	}

	type TableRow struct {
		TableName string
		Link      string
	}
	var Tables []TableRow
	for _, tb := range tables {
		Tables = append(Tables, TableRow{TableName: tb.TableName})
	}

	ctx.ViewData("Tables", Tables)
	ctx.View("index.html")
}
