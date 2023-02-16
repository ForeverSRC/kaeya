package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ForeverSRC/kaeya/pkg/api/rest"
	"github.com/ForeverSRC/kaeya/pkg/application"
	"github.com/ForeverSRC/kaeya/pkg/config"
	"github.com/ForeverSRC/kaeya/pkg/logger"
)

func main() {
	conf := initGlobalDependency()

	app, err := application.NewApplication(conf)
	if err != nil {
		panic(err)
	}

	server := rest.CreateHttpServer(app)

	go func() {
		if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Logger.Error().Err(err).Msg("http server serve error")
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err = server.Shutdown(ctx); err != nil {
		logger.Logger.Error().Err(err).Msg("http server shutdown error")
	}

	if err = app.Close(ctx); err != nil {
		logger.Logger.Error().Err(err).Msg("application shutdown error")
	}

	logger.Logger.Info().Msg("application closed")

}

func initGlobalDependency() config.KaeyaConfig {
	conf, err := config.ProvideConfig()
	if err != nil {
		panic(err)
	}

	err = logger.InitZerolog(conf.Log.Level)
	if err != nil {
		panic(err)
	}

	return conf
}
