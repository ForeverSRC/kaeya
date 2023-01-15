package segment

import (
	"context"
	"path"
	"time"

	"github.com/ForeverSRC/kaeya/pkg/domain"
	"github.com/ForeverSRC/kaeya/pkg/logger"
	codec2 "github.com/ForeverSRC/kaeya/pkg/storage/codec"
	"github.com/ForeverSRC/kaeya/pkg/storage/system/segment/mananger"
)

const (
	defaultSegmentBufferSize = 1024
	defaultMergeFloor        = 1024
	defaultRefreshInterval   = 1 * time.Second
	defaultFlushInterval     = 15 * time.Second
	defaultMergeInterval     = 30 * time.Second
)

type FSOpts struct {
	refreshInterval time.Duration
	flushInterval   time.Duration
	mergeInterval   time.Duration
	writeBufferSize int64
	mergeFloor      int64
}

type Option func(opts *FSOpts)

func WithMaxBufferSize(size int64) Option {
	return func(opts *FSOpts) {
		opts.writeBufferSize = size
	}
}

func WithRefreshInterval(interval time.Duration) Option {
	return func(opts *FSOpts) {
		opts.refreshInterval = interval
	}
}

func WithFlushInterval(interval time.Duration) Option {
	return func(opts *FSOpts) {
		opts.flushInterval = interval
	}
}

func WithMergeInterval(interval time.Duration) Option {
	return func(opts *FSOpts) {
		opts.mergeInterval = interval
	}
}

func WithMergeFloor(size int64) Option {
	return func(opts *FSOpts) {
		opts.mergeFloor = size
	}
}

type SegmentFSRepository struct {
	*FSOpts

	segmentManager  mananger.Manager
	segmentRootPath string

	refreshTicker *time.Ticker
	flushTicker   *time.Ticker
	mergeTicker   *time.Ticker
	stopCh        chan struct{}
}

func NewDefaultSegmentFSRepository(codec codec2.Codec, rootPath string, options ...Option) (*SegmentFSRepository, error) {
	opts := &FSOpts{
		refreshInterval: defaultRefreshInterval,
		flushInterval:   defaultFlushInterval,
		mergeInterval:   defaultMergeInterval,
		writeBufferSize: defaultSegmentBufferSize,
		mergeFloor:      defaultMergeFloor,
	}

	for _, op := range options {
		op(opts)
	}

	segPath := path.Join(rootPath, "data", "segments")
	segManager, err := mananger.NewSegmentManager(segPath, opts.writeBufferSize, opts.mergeFloor, codec)
	if err != nil {
		return nil, err
	}

	repo := &SegmentFSRepository{
		FSOpts:          opts,
		segmentManager:  segManager,
		segmentRootPath: segPath,
		refreshTicker:   time.NewTicker(opts.refreshInterval),
		flushTicker:     time.NewTicker(opts.flushInterval),
		mergeTicker:     time.NewTicker(opts.mergeInterval),
		stopCh:          make(chan struct{}),
	}

	go repo.backgroundWorker()

	return repo, nil
}

func (sr *SegmentFSRepository) Save(ctx context.Context, kv domain.KV) error {
	err := sr.segmentManager.Write(kv)
	if err != nil {
		return err
	}

	return nil
}

func (sr *SegmentFSRepository) backgroundWorker() {
	for {
		select {
		case <-sr.refreshTicker.C:
			err := sr.segmentManager.Refresh()
			if err != nil {
				logger.Logger.Error().Err(err).Msg("background refresh error")
			}

		case <-sr.flushTicker.C:
			err := sr.segmentManager.Flush()
			if err != nil {
				logger.Logger.Error().Err(err).Msg("background flush error")
			}
		case <-sr.mergeTicker.C:
			err := sr.segmentManager.Merge()
			if err != nil {
				logger.Logger.Error().Err(err).Msg("background merge error")
			}

		case <-sr.stopCh:
			return
		}
	}
}

func (sr *SegmentFSRepository) Load(ctx context.Context, key string) (domain.KV, error) {
	return sr.segmentManager.Read(key)
}

func (sr *SegmentFSRepository) Close(ctx context.Context) error {
	sr.stopCh <- struct{}{}
	return sr.segmentManager.Close()
}
