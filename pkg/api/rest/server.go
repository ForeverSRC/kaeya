package rest

import (
	"net/http"

	"github.com/ForeverSRC/kaeya/pkg/application"
)

func CreateHttpServer(app *application.Application) *http.Server {
	e := Route(app)
	server := &http.Server{
		Addr:    ":6666",
		Handler: e,
	}

	return server
}
