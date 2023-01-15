package mananger_test

import (
	"path"
	"testing"

	"github.com/ForeverSRC/kaeya/pkg/domain"
	"github.com/ForeverSRC/kaeya/pkg/storage/codec"
	"github.com/ForeverSRC/kaeya/pkg/storage/system/segment/mananger"
	"github.com/ForeverSRC/kaeya/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestSegmentNormal(t *testing.T) {
	cases := []struct {
		name   string
		values []domain.KV
	}{
		{
			name: "normal",
			values: []domain.KV{
				{"aaa", "1"},
				{"bb", "abdgeg"},
				{"ccccc", "100"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rootPath := path.Join("testdata", "dynamic", utils.ID())

			manager, err := mananger.NewSegmentManager(rootPath, 64, 1024, codec.NewStringCodec())
			assert.NoError(t, err)

			for _, kv := range c.values {
				err := manager.Write(kv)
				assert.NoError(t, err)
			}

			err = manager.Refresh()
			assert.NoError(t, err)

			for _, kv := range c.values {
				res, err := manager.Read(kv.Key)
				assert.NoError(t, err)
				assert.Equal(t, kv, res)
			}

			manager.Close()
		})
	}

}

func TestReadFromMultiSegment(t *testing.T) {
	cases := []struct {
		kv  domain.KV
		err error
	}{
		{
			kv:  domain.KV{"aaa", "1"},
			err: nil,
		},
		{
			kv:  domain.KV{"bb", "abdgeg"},
			err: nil,
		},
		{
			kv:  domain.KV{"ccccc", "100"},
			err: nil,
		},
		{
			kv:  domain.KV{"not-exist", ""},
			err: mananger.ErrNull,
		},
	}

	rootPath := path.Join("testdata", "static", "segments")
	manager, err := mananger.NewSegmentManager(rootPath, 64, 1024, codec.NewStringCodec())
	assert.NoError(t, err)
	defer manager.Close()

	for _, c := range cases {
		res, err := manager.Read(c.kv.Key)
		assert.Equal(t, c.err, err)
		assert.Equal(t, c.kv, res)
	}

}
