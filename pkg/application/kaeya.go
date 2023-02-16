package application

import (
	"context"

	"github.com/ForeverSRC/kaeya/pkg/config"
	"github.com/ForeverSRC/kaeya/pkg/service"
	"github.com/ForeverSRC/kaeya/pkg/storage"
)

type Application struct {
	DB service.DBService
}

func NewApplication(conf config.KaeyaConfig) (*Application, error) {
	repo, err := storage.NewStorage(conf.Storage)
	if err != nil {
		return nil, err
	}

	return &Application{
		DB: service.NewDefaultDBService(repo),
	}, nil
}

func (app *Application) Close(ctx context.Context) error {
	return app.DB.Close(ctx)
}
