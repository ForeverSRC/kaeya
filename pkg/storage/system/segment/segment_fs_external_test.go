package segment_test

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/ForeverSRC/kaeya/pkg/domain"
	codec2 "github.com/ForeverSRC/kaeya/pkg/storage/codec"
	"github.com/ForeverSRC/kaeya/pkg/storage/system/fs"
	"github.com/ForeverSRC/kaeya/pkg/storage/system/segment"
	"github.com/ForeverSRC/kaeya/pkg/utils"
	"github.com/stretchr/testify/assert"
)

type op string

const (
	opGet   op = "get"
	opSet   op = "set"
	opSleep op = "sleep"
)

type command struct {
	op       op
	kv       domain.KV
	interval time.Duration

	expect    domain.KV
	expectErr error
}

func TestSegmentFile(t *testing.T) {
	segmentFS, err := segment.NewDefaultSegmentFSRepository(
		codec2.NewStringCodec(),
		path.Join("testdata", "dynamic", "segment-test", utils.ID()),
		segment.WithFlushInterval(4*time.Second),
		segment.WithMaxBufferSize(128),
		segment.WithRefreshInterval(2*time.Second),
		segment.WithMergeFloor(1024),
		segment.WithMergeInterval(5*time.Second),
	)

	assert.NoError(t, err)

	commands := []command{
		{
			op:        opGet,
			kv:        domain.KV{"aaa", "1"},
			expect:    domain.KV{"aaa", ""},
			expectErr: fs.ErrNull,
		},
		{
			op: opSet,
			kv: domain.KV{"aaa", "1"},
		},
		{
			op: opSet,
			kv: domain.KV{"bb", "100"},
		},
		{
			op:        opGet,
			kv:        domain.KV{"aaa", "1"},
			expect:    domain.KV{"aaa", ""},
			expectErr: fs.ErrNull,
		},
		{
			op:       opSleep,
			interval: 3 * time.Second,
		},
		{
			op:     opGet,
			kv:     domain.KV{"aaa", "1"},
			expect: domain.KV{"aaa", "1"},
		},
		{
			op: opSet,
			kv: domain.KV{"aaa", "2"},
		},
		{
			op:     opGet,
			kv:     domain.KV{"aaa", "1"},
			expect: domain.KV{"aaa", "1"},
		},
		{
			op:       opSleep,
			interval: 3 * time.Second,
		},
		{
			op:     opGet,
			kv:     domain.KV{"aaa", "2"},
			expect: domain.KV{"aaa", "2"},
		},
		{
			op:     opGet,
			kv:     domain.KV{"bb", "100"},
			expect: domain.KV{"bb", "100"},
		},
	}

	ctx := context.Background()

	for _, cmd := range commands {
		println(cmd.op)

		switch cmd.op {
		case opGet:
			kv, err := segmentFS.Load(ctx, cmd.kv.Key)
			assert.Equal(t, cmd.expectErr, err)
			assert.Equal(t, cmd.expect, kv)
		case opSet:
			err := segmentFS.Save(ctx, cmd.kv)
			assert.Equal(t, cmd.expectErr, err)
		case opSleep:
			time.Sleep(cmd.interval)
		}
	}

	segmentFS.Close(ctx)

}
