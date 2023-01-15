package storage

import (
	"context"
	"fmt"

	"github.com/ForeverSRC/kaeya/pkg/config"
	"github.com/ForeverSRC/kaeya/pkg/domain"
	"github.com/ForeverSRC/kaeya/pkg/storage/codec"
	"github.com/ForeverSRC/kaeya/pkg/storage/index"
	"github.com/ForeverSRC/kaeya/pkg/storage/system"
	"github.com/ForeverSRC/kaeya/pkg/storage/system/fs"
	"github.com/ForeverSRC/kaeya/pkg/storage/system/segment"
	"github.com/ForeverSRC/kaeya/pkg/utils"
)

type Repository interface {
	Save(ctx context.Context, kv domain.KV) error
	Load(ctx context.Context, key string) (domain.KV, error)
	Close(ctx context.Context) error
}

func NewStorage(conf config.StorageConfig) (Repository, error) {
	cd, err := codec.NewCodec(conf.Codec)
	if err != nil {
		return nil, err
	}

	idxr := index.NewInMemoryIndexer()

	var repo Repository
	switch system.SystemKind(conf.System) {
	case system.KindFS:
		repo, err = fs.NewFileSystemRepository(cd, idxr, conf.Path)
	case system.KindSegment:
		sf := conf.Segment
		options := make([]segment.Option, 0)

		if sf.BufferSize != "" {
			bufSize, err := utils.ToBytes(sf.BufferSize)
			if err != nil {
				return nil, err
			}
			options = append(options, segment.WithMaxBufferSize(bufSize))
		}

		if sf.MergeFloor != "" {
			mergeFloor, err := utils.ToBytes(sf.MergeFloor)
			if err != nil {
				return nil, err
			}
			options = append(options, segment.WithMaxBufferSize(mergeFloor))
		}

		if sf.RefreshInterval != "" {
			d, err := utils.ParseDuration(sf.RefreshInterval)
			if err != nil {
				return nil, err
			}

			options = append(options, segment.WithRefreshInterval(d))
		}

		if sf.FlushInterval != "" {
			d, err := utils.ParseDuration(sf.FlushInterval)
			if err != nil {
				return nil, err
			}

			options = append(options, segment.WithFlushInterval(d))
		}

		repo, err = segment.NewDefaultSegmentFSRepository(cd, conf.Path, options...)
	default:
		err = fmt.Errorf("no such kind of system: %s", conf.System)
	}

	if err != nil {
		return nil, err
	}

	return repo, nil

}
