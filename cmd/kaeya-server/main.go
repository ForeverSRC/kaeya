package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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

	go func() {
		if err = server.ListenAndServe(); err != nil {
			fmt.Printf("http server serve error %v\n", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err = server.Shutdown(ctx); err != nil {
		fmt.Printf("http server shutdown error %v\n", err)
	}

	if err = app.Close(ctx); err != nil {
		fmt.Printf("application shutdown error %v\n", err)
	}

	fmt.Println("application closed.")

}
