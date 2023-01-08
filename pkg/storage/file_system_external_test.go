package storage_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ForeverSRC/kaeya/pkg/domain"
	"github.com/stretchr/testify/assert"

	"github.com/ForeverSRC/kaeya/pkg/storage"
	"github.com/ForeverSRC/kaeya/pkg/storage/codec"
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
			cd := codec.NewStringCodec()

			fs, err := storage.NewFileSystemRepository(cd, "testdata")
			assert.NoError(t, err)

			ctx := context.Background()

			for _, kv := range c.kvs {
				err = fs.Save(ctx, kv)
				assert.NoError(t, err)
			}

			for _, e := range c.expected {
				kv, err := fs.Load(ctx, e.Key)
				assert.NoError(t, err)
				assert.Equal(t, e, kv)
			}

			fs.Close(context.Background())
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
			cd := codec.NewStringCodec()

			fs, err := storage.NewFileSystemRepository(cd, "testdata")
			assert.NoError(t, err)

			ctx := context.Background()

			for _, kv := range c.data {
				err = fs.Save(ctx, kv)
				assert.NoError(t, err)

				res, err := fs.Load(ctx, kv.Key)
				assert.NoError(t, err)
				assert.Equal(t, kv, res)
			}

			fs.Close(context.Background())
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

	cd := codec.NewStringCodec()

	fs, err := storage.NewFileSystemRepository(cd, "testdata")
	assert.NoError(t, err)

	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, kv := range data {
			err = fs.Save(ctx, kv)
			assert.NoError(t, err)
			time.Sleep(500 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range data {
			err = fs.Save(ctx, data[len(data)-i-1])
			assert.NoError(t, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range data {
			kv, err := fs.Load(ctx, data[i].Key)
			assert.NoError(t, err)
			assert.Equal(t, data[i].Key, kv.Key)
			println(kv.Key, ":", kv.Value)
			time.Sleep(300 * time.Millisecond)
		}
	}()

	wg.Wait()

	println("--------------")
	for i := range data {
		kv, err := fs.Load(ctx, data[i].Key)
		assert.NoError(t, err)
		println(kv.Key, ":", kv.Value)
	}

	fs.Close(context.Background())

}
