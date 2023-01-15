package utils_test

import (
	"errors"
	"testing"

	"github.com/ForeverSRC/kaeya/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestBytesConvert(t *testing.T) {
	cases := []struct {
		input    string
		expected int64
		err      error
	}{
		{"128b", 128, nil},
		{"2k", 2048, nil},
		{"00012k", 12 * 1024, nil},
		{"dfdsfb", 0, utils.ErrInvalidFormat},
		{"123t", 0, utils.ErrInvalidFormat},
	}

	for _, c := range cases {
		res, err := utils.ToBytes(c.input)
		if c.err != nil {
			assert.True(t, errors.Is(err, c.err))
		} else {
			assert.Equal(t, c.expected, res)
		}
	}
}
