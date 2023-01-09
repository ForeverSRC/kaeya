package storage

import (
	"context"

	"github.com/ForeverSRC/kaeya/pkg/domain"
)

type Encoder interface {
	Encode(ctx context.Context, value domain.KV) ([]byte, error)
}

type Decoder interface {
	Decode(ctx context.Context, bytes []byte) (domain.KV, error)
}

type Codec interface {
	Encoder
	Decoder
}
