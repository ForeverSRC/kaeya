package mananger

import (
	"testing"

	"github.com/ForeverSRC/kaeya/pkg/storage/codec"
	"github.com/stretchr/testify/assert"
)

func TestInitSegments(t *testing.T) {
	manager, err := NewSegmentManager("testdata/static/segments", 128, 1024, codec.NewStringCodec())
	assert.NoError(t, err)

	assert.Equal(t, 3, manager.linkList.maxID())
	assert.Equal(t, 1, manager.linkList.minID())

	ids := []int{3, 2, 1}
	iter := manager.linkList.iterator()
	for _, id := range ids {
		curr := iter.next()
		if curr == nil {
			t.Fatalf("segment not exist")
		}
		assert.Equal(t, id, curr.segmentID)
	}

	manager.Close()

}
