package common_test

import (
	"os"
	"path"
	"testing"

	"github.com/ForeverSRC/kaeya/pkg/storage/common"
	"github.com/stretchr/testify/assert"
)

func TestReadLineFromTail(t *testing.T) {
	file, err := os.OpenFile(path.Join("testdata", "data.txt"), os.O_RDWR, 0754)
	defer file.Close()

	assert.NoError(t, err)

	expected := [][]byte{
		[]byte(`ccc,{"a":1}`),
		[]byte(`bbb,cdggreg`),
		[]byte(`aaa,1`),
	}

	var offset int64 = -1
	for _, e := range expected {
		data, newOffset, err := common.ReadLineFromTail(file, offset, '\n')
		if err != nil {
			assert.Equal(t, common.ErrEnd, err)
		}

		assert.Equal(t, e, data)
		offset = newOffset
	}

	_, _, err = common.ReadLineFromTail(file, offset, '\n')
	assert.Equal(t, common.ErrEnd, err)

}
