package webui

import (
	"context"
	"time"

	"github.com/kataras/iris/v12"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartWebServer starts an iris-powered HTTP server.
func StartWebServer() {
	app := iris.New()
	app.Get("/", indexHandler)
	app.Get("/tables", tablesHandler)
	app.Get("/metrics", func(ctx iris.Context) {
		handler := promhttp.Handler()
		handler.ServeHTTP(ctx.ResponseWriter(), ctx.Request())
	})

	iris.RegisterOnInterrupt(func() {
		// gracefully shutdown on interrupt
		timeout := 5 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		app.Shutdown(ctx)
	})

	// Register the view engine to the views,
	// this will load the templates.
	tmpl := iris.HTML("./templates", ".html")
	tmpl.Reload(true)
	app.RegisterView(tmpl)

	go func() {
		app.Listen(":8080")
	}()
}
