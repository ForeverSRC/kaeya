package codec

import (
	"fmt"

	"github.com/ForeverSRC/kaeya/pkg/domain"
)

type Encoder interface {
	Encode(value domain.KV) ([]byte, error)
}

type Decoder interface {
	Decode(bytes []byte) (domain.KV, error)
}

type Codec interface {
	Encoder
	Decoder
}

type CodecType string

const (
	TypeCSV CodecType = "csv"
)

func NewCodec(kind string) (Codec, error) {
	switch CodecType(kind) {
	case TypeCSV:
		return NewStringCodec(), nil
	default:
		return nil, fmt.Errorf("no codec type: %s", kind)
	}
}
