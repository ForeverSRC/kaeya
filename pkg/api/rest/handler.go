package rest

import (
	"net/http"

	"github.com/ForeverSRC/kaeya/pkg/domain"
	"github.com/ForeverSRC/kaeya/pkg/service"
	"github.com/gin-gonic/gin"
)

func Set(db service.DBService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SetKVRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, NewErrorResponse(CodeBadRequest, err.Error()))
			return
		}

		kv := domain.KV{
			Key:   req.Key,
			Value: req.Value,
		}

		err := db.Set(c.Request.Context(), kv)
		if err != nil {
			c.JSON(http.StatusInternalServerError, NewErrorResponse(CodeInternalError, err.Error()))
			return
		}

		c.JSON(http.StatusOK, NewSuccessResponse("", nil))

	}
}

func Get(db service.DBService) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.Param("key")
		if key == "" {
			c.JSON(http.StatusOK, NewErrorResponse(CodeBadRequest, "empty key"))
			return
		}

		kv, err := db.Get(c.Request.Context(), key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, NewErrorResponse(CodeInternalError, err.Error()))
			return
		}

		c.JSON(http.StatusOK, NewSuccessResponse("", kv))

	}
}
