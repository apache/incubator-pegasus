package webui

import (
	"github.com/kataras/iris/v12"
)

func StartWebServer() {
	app := iris.New()
	app.Get("/", indexHandler)

	// Register the view engine to the views,
	// this will load the templates.
	tmpl := iris.HTML("./templates", ".html")
	tmpl.Reload(true)
	app.RegisterView(tmpl)

	go func() {
		app.Listen(":8080")
	}()
}
