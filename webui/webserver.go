package webui

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func StartWebServer() {
	engine := gin.Default()
	engine.LoadHTMLGlob("templates/*")

	engine.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{})
	})

	engine.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{
			"status": 404,
			"error":  "page not exists",
		})
	})

	go func() {
		engine.Run(":34111")
	}()
}
