package codec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ForeverSRC/kaeya/pkg/domain"
)

const (
	kvFormat = "%s,%s"
)

var (
	ErrDataFormat = errors.New("data format error")
)

type StringCodec struct {
	format string
}

func NewStringCodec() *StringCodec {
	return &StringCodec{
		format: kvFormat,
	}
}

func (s *StringCodec) Encode(ctx context.Context, value domain.KV) ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf(s.format, value.Key, value.Value))
	return buffer.Bytes(), nil
}

func (s *StringCodec) Decode(ctx context.Context, bytes []byte) (domain.KV, error) {
	var res domain.KV

	str := string(bytes)

	idx := strings.IndexByte(str, ',')
	if idx == -1 {
		return res, ErrDataFormat
	}

	res.Key = str[0:idx]
	res.Value = str[idx+1:]

	return res, nil

}
