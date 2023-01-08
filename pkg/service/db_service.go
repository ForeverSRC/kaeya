package service

import (
	"context"

	"github.com/ForeverSRC/kaeya/pkg/domain"
)

type DBService interface {
	Set(ctx context.Context, kv domain.KV) error
	Get(ctx context.Context, key string) (domain.KV, error)
	Close(ctx context.Context) error
}

type DefaultDBService struct {
	repo Repository
}

func NewDefaultDBService(repo Repository) *DefaultDBService {
	return &DefaultDBService{
		repo: repo,
	}
}

func (d *DefaultDBService) Set(ctx context.Context, kv domain.KV) error {
	return d.repo.Save(ctx, kv)
}

func (d *DefaultDBService) Get(ctx context.Context, key string) (domain.KV, error) {
	return d.repo.Load(ctx, key)
}

func (d *DefaultDBService) Close(ctx context.Context) error {
	return d.repo.Close(ctx)
}
