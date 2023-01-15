package mananger

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ForeverSRC/kaeya/pkg/domain"
	"github.com/ForeverSRC/kaeya/pkg/logger"
	codec2 "github.com/ForeverSRC/kaeya/pkg/storage/codec"
	"github.com/ForeverSRC/kaeya/pkg/storage/common"
	"github.com/ForeverSRC/kaeya/pkg/utils"
)

const (
	segmentFileNamePrefix    = "seg"
	segmentFileNameExtension = ".sgk"

	segmentFileNameNumDelim = "_"
	segmentFileDataDelim    = '\n'

	fileMode = 0754
)

var (
	ErrNull = errors.New("null value")
)

type segmentFile struct {
	*os.File
	segmentID int
	flushed   bool
	next      *segmentFile
	prev      *segmentFile
}

type Manager interface {
	Refresh() error
	Close() error
	Write(kv domain.KV) error
	Read(key string) (domain.KV, error)
	Flush() error
	Merge() error
}

type DefaultManager struct {
	segmentPath string
	mergeFloor  int64

	codec codec2.Codec

	writeLock   sync.Mutex
	writeBuffer *bytes.Buffer
	mergeBuffer *bytes.Buffer

	linkList *segmentLinkList
}

func NewSegmentManager(segmentPath string, writeBufferSize int64, mergeFloor int64, codec codec2.Codec) (*DefaultManager, error) {
	sm := &DefaultManager{
		segmentPath: segmentPath,
		codec:       codec,
		mergeFloor:  mergeFloor,
	}

	err := sm.initSegmentFiles()
	if err != nil {
		return nil, err
	}

	sm.writeBuffer = bytes.NewBuffer(make([]byte, 0, writeBufferSize))
	sm.mergeBuffer = bytes.NewBuffer(make([]byte, 0, writeBufferSize))

	return sm, nil
}

func (sm *DefaultManager) initSegmentFiles() error {
	if !utils.PathExists(sm.segmentPath) {
		err := os.MkdirAll(sm.segmentPath, fileMode)
		if err != nil {
			return err
		}
	}

	err := sm.getAllSegmentFiles()
	if err != nil {
		return err
	}

	return nil

}

func (sm *DefaultManager) getAllSegmentFiles() error {
	files := make([]string, 0)

	err := filepath.WalkDir(sm.segmentPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		name := d.Name()
		strs := strings.Split(name, "_")

		if strs[1] != (segmentFileNamePrefix + segmentFileNameExtension) {
			return nil
		}

		files = append(files, path)

		return nil

	})

	if err != nil {
		return fmt.Errorf("walk for segemnt error: %w", err)
	}
	if len(files) == 0 {
		sm.linkList = newLinkListFromSlice(0, 0, nil)
	} else {
		linkList, err := openSegmentFiles(files)
		if err != nil {
			return err
		}
		sm.linkList = linkList
	}

	return nil

}

func openSegmentFiles(files []string) (linkList *segmentLinkList, err error) {
	sgs := make([]*segmentFile, 0, len(files))

	defer func() {
		if err != nil {
			for _, f := range sgs {
				f.Close()
			}
		}
	}()

	for _, fName := range files {

		file, err := os.OpenFile(fName, os.O_RDONLY, fileMode)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("open file %s error: %w", fName, err)
		}

		reader := bufio.NewReader(file)
		l, _, err := reader.ReadLine()
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("read segmentID error: %w", err)
		}

		id, err := strconv.Atoi(string(l))
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("convert segmentID error: %w", err)
		}

		sg := &segmentFile{
			segmentID: id,
			File:      file,
			flushed:   true,
		}

		sgs = append(sgs, sg)

	}

	sort.Slice(sgs, func(i, j int) bool {
		return sgs[i].segmentID > sgs[j].segmentID
	})

	linkList = newLinkListFromSlice(sgs[0].segmentID, sgs[len(sgs)-1].segmentID, sgs)

	return linkList, nil

}

func getSegmentFileName() string {
	return fmt.Sprintf("%d%s%s%s", time.Now().UnixNano(), segmentFileNameNumDelim, segmentFileNamePrefix, segmentFileNameExtension)
}

func (sm *DefaultManager) Write(kv domain.KV) error {
	sm.writeLock.Lock()
	defer sm.writeLock.Unlock()

	data, err := sm.codec.Encode(kv)
	if err != nil {
		return err
	}

	data = append([]byte{segmentFileDataDelim}, data...)

	// if the buffer will be full after this write, doRefresh
	if sm.writeBuffer.Len()+len(data) > sm.writeBuffer.Cap() {
		err := sm.doRefresh()
		if err != nil {
			return err
		}
	}

	_, err = sm.writeBuffer.Write(data)
	return err
}

func (sm *DefaultManager) Close() error {
	sm.Refresh()
	sm.Flush()

	iter := sm.linkList.iterator()

	for iter.hasNext() {
		curr := iter.next()
		curr.Close()
	}

	return nil
}

func (sm *DefaultManager) Refresh() error {
	sm.writeLock.Lock()
	defer sm.writeLock.Unlock()

	if sm.writeBuffer.Len() == 0 {
		return nil
	}

	return sm.doRefresh()
}

func (sm *DefaultManager) doRefresh() error {
	newSegID := sm.linkList.maxID() + 1

	newSeg, err := newSegmentFile(sm.segmentFileFullPath(), newSegID, sm.writeBuffer.Bytes())
	if err != nil {
		return err
	}

	sm.writeBuffer.Reset()

	sm.linkList.addToHead(newSeg)

	return nil
}

func (sm *DefaultManager) segmentFileFullPath() string {
	return path.Join(sm.segmentPath, getSegmentFileName())
}

func newSegmentFile(filePath string, segmentID int, data []byte) (*segmentFile, error) {
	newFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, fileMode)
	if err != nil {
		return nil, err
	}

	metadata := []byte(fmt.Sprintf("%d\n", segmentID))
	content := append(metadata, data...)

	_, err = newFile.Write(content)
	if err != nil {
		return nil, err
	}

	err = newFile.Close()
	if err != nil {
		return nil, err
	}

	// read only
	newFile, err = os.Open(newFile.Name())
	if err != nil {
		return nil, fmt.Errorf("open new segment file error: %w", err)
	}

	newSeg := &segmentFile{
		File:      newFile,
		segmentID: segmentID,
		flushed:   false,
	}

	return newSeg, nil

}

func (sm *DefaultManager) Read(key string) (domain.KV, error) {
	iter := sm.linkList.iterator()

	for iter.hasNext() {
		curr := iter.next()
		kv, err := sm.loadFromFile(curr.File, key)
		if err != nil {
			continue
		}

		return kv, nil
	}

	return domain.KV{Key: key}, ErrNull
}

func (sm *DefaultManager) loadFromFile(file *os.File, key string) (domain.KV, error) {
	res := domain.KV{
		Key: key,
	}

	var offset int64 = -1

	for {
		data, newOffset, err := common.ReadLineFromTail(file, offset, segmentFileDataDelim)
		if err != nil {
			switch {
			case errors.Is(err, common.ErrEnd):
				return res, ErrNull
			case errors.Is(err, common.ErrEmptyLine):
				offset = newOffset
				continue
			default:
				return res, err
			}
		}

		kv, err := sm.codec.Decode(data)
		if err != nil {
			return res, err
		}

		if kv.Key == key {
			return kv, nil
		}

		offset = newOffset
	}
}

func (sm *DefaultManager) Flush() error {
	iter := sm.linkList.iterator()

	for iter.hasNext() {
		curr := iter.next()
		err := curr.Sync()
		if err != nil {
			return err

		}
	}

	return nil
}

func (sm *DefaultManager) Merge() error {
	sm.writeLock.Lock()
	defer sm.writeLock.Unlock()

	count := sm.linkList.count()
	if count <= 1 {
		return nil
	}

	states := make([]bool, 0, count)
	segs := make([]segmentFile, 0, count)
	canDoMerge := false

	iter := sm.linkList.iterator()
	for iter.hasNext() {
		canMerge := false
		s := iter.next()
		stat, err := s.Stat()
		if err != nil {
			logger.Logger.Error().Err(err).Msgf("read stat of file %s error", s.Name())
			canMerge = false
		} else {
			canMerge = stat.Size() <= sm.mergeFloor
		}

		canDoMerge = canDoMerge || canMerge
		states = append(states, canMerge)
		segs = append(segs, *s)
	}

	if !canDoMerge {
		return nil
	}

	newSegments := make([]*segmentFile, 0, count/2)
	needDeleted := make([]segmentFile, 0, count/2)

	for i := 0; i < count; {
		if i == count-1 {
			newSegments = append(newSegments, &segs[i])
			break
		}

		if states[i] {
			if states[i+1] {
				merged, err := sm.doMerge(segs[i], segs[i+1])
				if err != nil {
					return err
				}
				newSegments = append(newSegments, merged)
				needDeleted = append(needDeleted, segs[i], segs[i+1])
			} else {
				newSegments = append(newSegments, &segs[i], &segs[i+1])
			}
			i += 2
		} else {
			newSegments = append(newSegments, &segs[i])
			i++
		}
	}

	sort.Slice(newSegments, func(i, j int) bool {
		return newSegments[i].segmentID > newSegments[j].segmentID
	})

	newList := newLinkListFromSlice(newSegments[0].segmentID, newSegments[len(newSegments)-1].segmentID, newSegments)

	sm.linkList = newList

	// remove segments
	for _, s := range needDeleted {
		err := s.Close()
		if err != nil {
			logger.Logger.Warn().Err(err).Msgf("close file %s failed", s.Name())
		}
		err = os.Remove(s.Name())
		if err != nil {
			logger.Logger.Warn().Err(err).Msgf("remove file %s failed", s.Name())
		}
	}

	return nil

}

func (sm *DefaultManager) doMerge(prev, next segmentFile) (*segmentFile, error) {
	prevData, err := sm.readAllData(prev)
	if err != nil {
		return nil, err
	}

	nextData, err := sm.readAllData(next)
	if err != nil {
		return nil, err
	}

	tmpMerged := append(prevData, nextData...)
	res := make([]domain.KV, 0, len(prevData)+len(nextData))
	hash := make(map[string]bool, len(prevData)+len(nextData))

	for _, kv := range tmpMerged {
		if !hash[kv.Key] {
			res = append(res, kv)
			hash[kv.Key] = true
		}
	}

	// guarantee empty
	sm.mergeBuffer.Reset()

	for i := len(res) - 1; i >= 0; i-- {
		data, err := sm.codec.Encode(res[i])
		if err != nil {
			logger.Logger.Error().Err(err).Msgf("encode key [%s] error, discard", res[i].Key)
			continue
		}

		data = append([]byte{segmentFileDataDelim}, data...)
		sm.mergeBuffer.Write(data)
	}

	seg, err := newSegmentFile(sm.segmentFileFullPath(), next.segmentID, sm.mergeBuffer.Bytes())
	// guarantee empty
	sm.mergeBuffer.Reset()

	if err != nil {
		return nil, err
	}

	return seg, nil

}

func (sm *DefaultManager) readAllData(segment segmentFile) ([]domain.KV, error) {
	res := make([]domain.KV, 0, 10)
	var offset int64 = -1

	for {
		data, newOffset, err := common.ReadLineFromTail(segment.File, offset, segmentFileDataDelim)
		if err != nil {
			switch {
			case errors.Is(err, common.ErrEnd):
				return res, nil
			case errors.Is(err, common.ErrEmptyLine):
				return res, nil
			default:
				return nil, err
			}
		}

		kv, err := sm.codec.Decode(data)
		if err != nil {
			logger.Logger.Error().Err(err).Msg("decode error, discard")
			continue
		}
		res = append(res, kv)

		offset = newOffset
	}
}
