package fs_test

import (
	"context"
	"errors"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/ForeverSRC/kaeya/pkg/domain"
	"github.com/ForeverSRC/kaeya/pkg/storage/index"
	"github.com/ForeverSRC/kaeya/pkg/storage/system/fs"
	"github.com/ForeverSRC/kaeya/pkg/utils"
	"github.com/stretchr/testify/assert"

	"github.com/ForeverSRC/kaeya/pkg/storage/codec"
)

const (
	testRoot = "testdata"
)

var (
	testDynamicRoot = path.Join(testRoot, "dynamic")
	testStaticRoot  = path.Join(testRoot, "static")
)

func TestNormal(t *testing.T) {
	cases := []struct {
		name     string
		kvs      []domain.KV
		expected []domain.KV
	}{
		{
			name: "normal",
			kvs: []domain.KV{
				{Key: "aaa", Value: "1"},
				{Key: "bbb", Value: "2"},
				{Key: "ccc", Value: `{"a":1,"b":2}`},
				{Key: "aaa", Value: "10"},
				{Key: "bbb", Value: "hhh"},
			},
			expected: []domain.KV{
				{Key: "aaa", Value: "10"},
				{Key: "bbb", Value: "hhh"},
				{Key: "ccc", Value: `{"a":1,"b":2}`},
				{Key: "hhh"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rootPath := path.Join(testDynamicRoot, utils.ID())

			cd := codec.NewStringCodec()
			indexer := index.NewInMemoryIndexer()

			fr, err := fs.NewFileSystemRepository(cd, indexer, rootPath)
			assert.NoError(t, err)

			ctx := context.Background()

			for _, kv := range c.kvs {
				err = fr.Save(ctx, kv)
				assert.NoError(t, err)
			}

			for _, e := range c.expected {
				kv, err := fr.Load(ctx, e.Key)
				if !errors.Is(err, fs.ErrNull) {
					assert.NoError(t, err)
				}

				assert.Equal(t, e, kv)

			}

			fr.Close(context.Background())
		})
	}

}

func TestSaveAndLoad(t *testing.T) {
	cases := []struct {
		name string
		data []domain.KV
	}{
		{
			name: "s-l",
			data: []domain.KV{
				{Key: "aa", Value: "1"},
				{Key: "bc", Value: "aaa-sss-bd"},
				{Key: "123c", Value: "100"},
				{Key: "aa", Value: "100"},
				{Key: "123c", Value: "90"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rootPath := path.Join(testDynamicRoot, utils.ID())

			cd := codec.NewStringCodec()
			indexer := index.NewInMemoryIndexer()

			fr, err := fs.NewFileSystemRepository(cd, indexer, rootPath)
			assert.NoError(t, err)

			ctx := context.Background()

			for _, kv := range c.data {
				err = fr.Save(ctx, kv)
				assert.NoError(t, err)

				res, err := fr.Load(ctx, kv.Key)
				if !errors.Is(err, fs.ErrNull) {
					assert.NoError(t, err)
				}
				assert.Equal(t, kv, res)
			}

			fr.Close(context.Background())
		})
	}

}

func TestConcurrentReadWrite(t *testing.T) {

	data := []domain.KV{
		{Key: "aa", Value: "1"},
		{Key: "bc", Value: "aaa-sss-bd"},
		{Key: "123c", Value: "100"},
		{Key: "aa", Value: "100"},
		{Key: "123c", Value: "90"},
	}

	rootPath := path.Join(testDynamicRoot, utils.ID())

	cd := codec.NewStringCodec()
	indexer := index.NewInMemoryIndexer()

	fr, err := fs.NewFileSystemRepository(cd, indexer, rootPath)
	assert.NoError(t, err)

	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, kv := range data {
			err = fr.Save(ctx, kv)
			assert.NoError(t, err)
			time.Sleep(500 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range data {
			err = fr.Save(ctx, data[len(data)-i-1])
			assert.NoError(t, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range data {
			kv, err := fr.Load(ctx, data[i].Key)
			if !errors.Is(err, fs.ErrNull) {
				assert.NoError(t, err)
			}

			assert.Equal(t, data[i].Key, kv.Key)
			println(kv.Key, ":", kv.Value)
			time.Sleep(300 * time.Millisecond)
		}
	}()

	wg.Wait()

	println("--------------")
	for i := range data {
		kv, err := fr.Load(ctx, data[i].Key)
		if !errors.Is(err, fs.ErrNull) {
			assert.NoError(t, err)
		}
		println(kv.Key, ":", kv.Value)
	}

	fr.Close(context.Background())

}

func TestInitIndex(t *testing.T) {
	rootPath := path.Join(testStaticRoot, "init-index")

	cd := codec.NewStringCodec()
	indexer := index.NewInMemoryIndexer()

	fr, err := fs.NewFileSystemRepository(cd, indexer, rootPath)
	assert.NoError(t, err)

	ctx := context.Background()

	existing := []domain.KV{
		{"aaa", "10"},
		{"bbb", "hhh"},
		{"ccc", `{"a":1,"b":2}`},
	}

	for _, kv := range existing {
		res, err := fr.Load(ctx, kv.Key)
		if !errors.Is(err, fs.ErrNull) {
			assert.NoError(t, err)
		}
		assert.Equal(t, kv.Value, res.Value)
	}

}
