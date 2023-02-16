package common

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	ErrEnd       = errors.New("offset beyond file size")
	ErrEmptyLine = errors.New("empty line")
)

func ReadLineFromTail(file *os.File, offset int64, delim byte) (line []byte, newOffset int64, err error) {
	fs, err := file.Stat()
	if err != nil {
		return nil, -1, fmt.Errorf("get file info error: %w", err)
	}

	size := fs.Size()

	buf := make([]byte, 1)
	lineBuf := bytes.NewBuffer(make([]byte, 0, 256))

	if -offset > size {
		return nil, offset, ErrEnd
	}

	for -offset <= size {
		ret, err := file.Seek(offset, io.SeekEnd)
		if err != nil {
			return nil, offset, fmt.Errorf("seek error: %w", err)
		}

		_, err = file.ReadAt(buf, ret)
		if err != nil {
			return nil, offset, fmt.Errorf("read error: %w", err)
		}

		// finish and skip delim for next reading
		if buf[0] == delim {
			offset--
			break
		} else {
			lineBuf.Write(buf)
			offset--
		}

	}

	data := Reverse(lineBuf.Bytes())
	if len(data) == 0 {
		return data, offset, ErrEmptyLine
	}

	return data, offset, nil
}

func Reverse(data []byte) []byte {
	n := len(data)
	res := make([]byte, n)
	for i := 0; i < n; i++ {
		res[i] = data[n-1-i]
	}

	return res
}
