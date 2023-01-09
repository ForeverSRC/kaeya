package storage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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

	lineDelim    = '\n'
	lineDelimLen = 1
)

var (
	errIndexNotFound = errors.New("not found by index")
	ErrNull          = errors.New("null vaule")
)

type FileSystemRepository struct {
	file    *os.File
	codec   Codec
	indexer Indexer
}

func NewFileSystemRepository(codec Codec, indexer Indexer, rootPath string) (*FileSystemRepository, error) {
	fs := &FileSystemRepository{
		codec:   codec,
		indexer: indexer,
	}

	f, err := fs.initFile(rootPath)
	if err != nil {
		return nil, fmt.Errorf("fs inti error: %w", err)
	}

	fs.file = f

	fs.initIndex()

	return fs, nil

}

func (fr *FileSystemRepository) initFile(rootPath string) (*os.File, error) {
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

	return file, nil

}

func (fr *FileSystemRepository) initIndex() {
	ctx := context.Background()

	scanner := bufio.NewScanner(fr.file)

	var offset int64 = 0
	for scanner.Scan() {
		data := scanner.Bytes()
		kv, err := fr.codec.Decode(ctx, data)
		if err == nil {
			_ = fr.indexer.Index(ctx, kv.Key, offset)
		}

		offset += int64(len(data)) + lineDelimLen
	}
}

func (fr *FileSystemRepository) Save(ctx context.Context, kv domain.KV) error {
	data, err := fr.codec.Encode(ctx, kv)
	if err != nil {
		return fmt.Errorf("encode error: %w", err)
	}

	data = append(data, lineDelim)
	n, err := fr.file.Write(data)

	if err != nil {
		return fmt.Errorf("write to file error: %w", err)
	}

	err = fr.file.Sync()
	if err != nil {
		return fmt.Errorf("sync to fs error: %w", err)
	}

	ret, err := fr.file.Seek(int64(-n), io.SeekEnd)
	if err != nil {
		return err
	}

	err = fr.indexer.Index(ctx, kv.Key, ret)
	if err != nil {
		return err
	}

	return nil

}

func (fr *FileSystemRepository) Load(ctx context.Context, key string) (domain.KV, error) {
	kv, err := fr.loadByIndex(ctx, key)
	if err != nil {
		if errors.Is(err, errIndexNotFound) {
			return fr.loadFromFile(ctx, key)
		} else {
			return domain.KV{Key: key}, err
		}
	}

	return kv, nil
}

func (fr *FileSystemRepository) loadByIndex(ctx context.Context, key string) (domain.KV, error) {
	res := domain.KV{
		Key: key,
	}

	offset, err := fr.indexer.Search(ctx, key)

	if err != nil {
		if errors.Is(err, ErrIndexMiss) {
			return res, errIndexNotFound
		} else {
			return res, err
		}

	}

	_, err = fr.file.Seek(offset, io.SeekStart)
	if err != nil {
		return res, err
	}

	reader := bufio.NewReader(fr.file)

	data, err := reader.ReadBytes(lineDelim)
	if err != nil {
		return res, err
	}

	kv, err := fr.codec.Decode(ctx, data[:len(data)-1])
	if err != nil {
		return res, err
	}

	if kv.Key != key {
		return res, errIndexNotFound
	}

	return kv, nil

}

func (fr *FileSystemRepository) loadFromFile(ctx context.Context, key string) (domain.KV, error) {
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

		if buf[0] == lineDelim {
			kv, err := fr.parseLine(ctx, lineBuf)
			if err == nil && kv.Key == key {
				// update index
				_ = fr.indexer.Index(ctx, kv.Key, ret)
				return kv, nil
			}
		} else {
			lineBuf.Write(buf)
		}

		offset--
	}

	kv, err := fr.parseLine(ctx, lineBuf)
	if err == nil && kv.Key == key {
		_ = fr.indexer.Index(ctx, kv.Key, 0)
		return kv, nil
	}

	return res, ErrNull
}

func (fr *FileSystemRepository) parseLine(ctx context.Context, lineBuf *bytes.Buffer) (domain.KV, error) {
	data := utils.Reverse(lineBuf.Bytes())
	lineBuf.Reset()
	return fr.codec.Decode(ctx, data)

}

func (fr *FileSystemRepository) Close(ctx context.Context) error {
	return fr.file.Close()
}
