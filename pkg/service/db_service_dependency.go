package service

import (
	"context"

	"github.com/ForeverSRC/kaeya/pkg/domain"
)

type Repository interface {
	Save(ctx context.Context, kv domain.KV) error
	Load(ctx context.Context, key string) (domain.KV, error)
	Close(ctx context.Context) error
}
