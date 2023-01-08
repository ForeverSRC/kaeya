package rest

import (
	"github.com/ForeverSRC/kaeya/pkg/application"
	"github.com/gin-gonic/gin"
)

func Route(app *application.Application) *gin.Engine {
	router := gin.New()

	router.POST("/kv", Set(app.DB))
	router.GET("/kv/:key", Get(app.DB))

	return router
}
