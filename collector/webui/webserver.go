// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package webui

import (
	"context"
	"time"

	"github.com/apache/incubator-pegasus/collector/metrics"
	"github.com/kataras/iris/v12"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartWebServer starts an iris-powered HTTP server.
func StartWebServer(registry prometheus.Registerer) {
	for _, cV := range metrics.CounterMetricsMap {
		registry.MustRegister(cV)
	}
	for _, gV := range metrics.GaugeMetricsMap {
		registry.MustRegister(gV)
	}

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
		err := app.Shutdown(ctx)
		if err != nil {
			return
		}
	})

	// Register the view engine to the views,
	// this will load the templates.
	tmpl := iris.HTML("./templates", ".html")
	tmpl.Reload(true)
	app.RegisterView(tmpl)

	go func() {
		err := app.Listen(":8080")
		if err != nil {
			return
		}
	}()
}
