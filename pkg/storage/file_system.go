package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/ForeverSRC/kaeya/pkg/domain"
	"github.com/ForeverSRC/kaeya/pkg/utils"
)

const (
	storageFileName          = "data"
	storageFileNameExtension = ".ky"
)

type FileSystemRepository struct {
	file  *os.File
	path  string
	codec Codec
}

func NewFileSystemRepository(codec Codec, rootPath string) (*FileSystemRepository, error) {
	if rootPath == "" {
		dir, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("fs repo inti: path empty, getWd error: %w", err)
		}

		rootPath = dir
	}

	filePath := path.Join(rootPath, "data")

	var file *os.File
	var err error

	for {
		file, err = os.OpenFile(path.Join(filePath, storageFileName+storageFileNameExtension), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0754)
		if err != nil {
			if os.IsNotExist(err) {
				err = os.MkdirAll(filePath, 0754)
				if err != nil {
					return nil, fmt.Errorf("fs repo inti: mkdir all error: %w", err)
				}

				continue

			} else {
				return nil, fmt.Errorf("init file system, open file error: %w", err)
			}

		}
		break
	}

	return &FileSystemRepository{
		file:  file,
		path:  filePath,
		codec: codec,
	}, nil
}

func (fr *FileSystemRepository) Save(ctx context.Context, kv domain.KV) error {
	data, err := fr.codec.Encode(ctx, kv)
	if err != nil {
		return fmt.Errorf("encode error: %w", err)
	}

	_, err = fr.file.Write(data)
	if err != nil {
		return fmt.Errorf("write to file error: %w", err)
	}

	err = fr.file.Sync()
	if err != nil {
		return fmt.Errorf("sync to fs error: %w", err)
	}

	return nil

}

func (fr *FileSystemRepository) Load(ctx context.Context, key string) (domain.KV, error) {
	res := domain.KV{
		Key: key,
	}

	fs, err := fr.file.Stat()
	if err != nil {
		return res, fmt.Errorf("get file info error: %w", err)
	}

	size := fs.Size()
	var offset int64 = -1

	buf := make([]byte, 1)
	lineBuf := bytes.NewBuffer(make([]byte, 0, 1024))

	for -offset <= size {
		ret, err := fr.file.Seek(offset, io.SeekEnd)
		if err != nil {
			return res, fmt.Errorf("seek error: %w", err)
		}

		_, err = fr.file.ReadAt(buf, ret)
		if err != nil {
			return res, fmt.Errorf("read error: %w", err)
		}

		if buf[0] == '\n' {
			kv, err := fr.parseLine(ctx, lineBuf)
			if err == nil && kv.Key == key {
				return kv, nil
			}
		} else {
			lineBuf.Write(buf)
		}

		offset--
	}

	kv, err := fr.parseLine(ctx, lineBuf)
	if err == nil && kv.Key == key {
		return kv, nil
	}

	return res, nil

}

func (fr *FileSystemRepository) parseLine(ctx context.Context, lineBuf *bytes.Buffer) (domain.KV, error) {
	data := utils.Reverse(lineBuf.Bytes())
	lineBuf.Reset()
	return fr.codec.Decode(ctx, data)

}

func (fr *FileSystemRepository) Close(ctx context.Context) error {
	return fr.file.Close()
}
