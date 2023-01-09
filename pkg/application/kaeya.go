package application

import (
	"context"
	"fmt"

	"github.com/ForeverSRC/kaeya/pkg/service"
	"github.com/ForeverSRC/kaeya/pkg/storage"
	"github.com/ForeverSRC/kaeya/pkg/storage/codec"
)

type Application struct {
	DB service.DBService
}

func NewApplication(filePath string) (*Application, error) {
	fsRepo, err := storage.NewFileSystemRepository(codec.NewStringCodec(), filePath)
	if err != nil {
		return nil, fmt.Errorf("create app error %w", err)
	}

	return &Application{
		DB: service.NewDefaultDBService(fsRepo),
	}, nil
}

func (app *Application) Close(ctx context.Context) error {
	return app.DB.Close(ctx)
}
