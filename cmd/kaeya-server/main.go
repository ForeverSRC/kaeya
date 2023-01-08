package main

import (
	"context"
	"os"
	"time"

	"github.com/ForeverSRC/kaeya/pkg/api/rest"
	"github.com/ForeverSRC/kaeya/pkg/application"
)

func main() {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	println("current dir:", wd)

	app, err := application.NewApplication(wd)
	if err != nil {
		panic(err)
	}

	server := rest.CreateHttpServer(app)

	if err = server.ListenAndServe(); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app.Close(ctx)

}
