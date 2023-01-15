package mananger

type segmentLinkList struct {
	dummyHead    *segmentFile
	tail         *segmentFile
	maxSegmentID int
	minSegmentID int
	num          int
}

func newLinkListFromSlice(maxID, minID int, segments []*segmentFile) *segmentLinkList {
	head := new(segmentFile)
	curr := head

	for _, sg := range segments {
		curr.next = sg
		sg.prev = curr
		curr = curr.next
	}

	tail := curr.prev

	return &segmentLinkList{
		dummyHead:    head,
		tail:         tail,
		maxSegmentID: maxID,
		minSegmentID: minID,
		num:          len(segments),
	}
}

func (sl *segmentLinkList) addToHead(seg *segmentFile) {
	if sl.dummyHead.next == nil {
		seg.prev = sl.dummyHead
		sl.dummyHead.next = seg
	} else {
		seg.prev = sl.dummyHead
		seg.next = sl.dummyHead.next
		seg.next.prev = seg
		sl.dummyHead.next = seg
	}

	sl.maxSegmentID = seg.segmentID
	sl.num++
}

func (sl *segmentLinkList) maxID() int {
	return sl.maxSegmentID
}

func (sl *segmentLinkList) minID() int {
	return sl.minSegmentID
}

func (sl *segmentLinkList) count() int {
	return sl.num
}

func (sl *segmentLinkList) iterator() *segmentLinkListIterator {
	return &segmentLinkListIterator{
		curr: sl.dummyHead,
	}
}

type segmentLinkListIterator struct {
	curr *segmentFile
}

func (i *segmentLinkListIterator) hasNext() bool {
	if i.curr == nil || i.curr.next == nil {
		return false
	}

	return true
}

func (i *segmentLinkListIterator) hasPrev() bool {
	if i.curr == nil || i.curr.prev == nil {
		return false
	}

	return true
}

func (i *segmentLinkListIterator) next() *segmentFile {
	if i.curr == nil {
		return nil
	}

	res := i.curr.next
	i.curr = i.curr.next

	return res
}

func (i *segmentLinkListIterator) prev() *segmentFile {
	if i.curr == nil {
		return nil
	}

	res := i.curr.prev
	i.curr = i.curr.prev

	return res
}
