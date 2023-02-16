package utils

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

const (
	Byte = "b"
	KB   = "k"
	MB   = "m"
	GB   = "g"

	Units = Byte + KB + MB + GB
)

const (
	b = 1 << (10 * iota)
	k
	m
	g
)

var (
	expr = "^[0-9]+(" + strings.Join([]string{Byte, KB, MB, GB}, "|") + ")$"

	ErrInvalidFormat = errors.New("invalid format")
	ErrInvalidUnit   = errors.New("invalid unit")
)

var regExp *regexp.Regexp

func init() {
	reg, err := regexp.Compile(expr)
	if err != nil {
		panic(err)
	}

	regExp = reg
}

func ToBytes(str string) (int64, error) {
	if !regExp.MatchString(str) {
		return 0, ErrInvalidFormat
	}

	pos := 0
	end := len(str)

	var num string
	var value string

	for i := pos; ; i++ {
		if i >= end {
			num = "0"
			value = num
			break
		}
		switch str[i] {
		case '0':
			pos++
		default:
			break
		}
	}

	for i := pos; ; i++ {
		if i >= end {
			num = str[pos:end]
			value = str[0:end]
			break
		}
		switch str[i] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		default:
			num = str[pos:i]
			pos = i
			break
		}
	}

	// if we stripped all numerator positions, always return 0
	if len(num) == 0 {
		num = "0"
	}

	value = str[0:pos]

	var suffix string
	suffixStart := pos
	for i := pos; ; i++ {
		if i >= end {
			suffix = str[suffixStart:end]
			break
		}
		if !strings.ContainsAny(str[i:i+1], Units) {
			pos = i
			break
		}
	}

	if pos != end-1 {
		return 0, ErrInvalidFormat
	}

	numInt, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}

	switch suffix {
	case Byte:
		return int64(numInt * b), nil
	case KB:
		return int64(numInt * k), nil
	case MB:
		return int64(numInt * m), nil
	case GB:
		return int64(numInt * g), nil
	default:
		return 0, ErrInvalidUnit
	}

}
