package ringbuffer

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

var ErrEmpty = errors.New("Empty")
var ErrFull = errors.New("Full")

// 32 bit
// len >= 0, mean len of the Element buf
// len < 0, mean skip abs(len) bytes
var ElementMetaSize uint64 = 4

// only support one Reader, one Writer !!!
type Queue struct {
	// vitrual ptr: writePtr alwarys >= readPtr
	readPtr, writePtr uint64

	// real cap is less than len(mem) by 4 byte
	// because the tail padding meta may over flow the mem,
	// so we cal avoid it.
	cap uint64

	// effactive mem: [readPtr, writePtr)
	mem []byte
}

// Design
//
// .....[meta1][data1]...[metaN][dataN].....(end)
//        ^                            ^
//      ReadPtr                     WritePtr
//
// ...[metaN][dataN]............[meta1][data1]...[skip N bytes](end)
//                  ^           ^
//               WritePtr     ReadPtr
//
func NewQueue(cap uint64) *Queue {
	return &Queue{
		readPtr:  0,
		writePtr: 0,
		cap:      cap,
		mem:      make([]byte, cap, cap+3), // extra 3 byte for overflow of the skip meta
	}
}

func (q *Queue) IsEmpty() bool {
	return q.readPtr == q.writePtr
}

// design principle: write only once
// if tail space is not enough or head space is not enough, return full
//
// special case:
//   [empty space: 9 bytes][.............][empty space: 8 bytes]
//                         ^ readPtr      ^ writePtr
//   4 bytes meta for a msg, if next put msg > 5 bytes, the queue will return ErrFull!
//
func (q *Queue) IsFull(bufSize uint64) bool {
	if (q.writePtr+ElementMetaSize+bufSize)-q.readPtr > uint64(len(q.mem)) {
		return true
	}

	cost := bufSize + ElementMetaSize

	phyReadPos := q.pos(q.readPtr)
	phyWritePos := q.pos(q.writePtr)
	if phyReadPos <= phyWritePos {
		// append to space after tail
		if phyWritePos+cost <= uint64(len(q.mem)) {
			return false
		}
		// append to space before head
		if cost <= phyReadPos {
			return false
		}
		return true
	} else {
		// phyWritePos < phyReadPos
		return phyWritePos+cost > phyReadPos
	}
}

// calc the read pos in memory
func (q *Queue) pos(ptr uint64) uint64 {
	return ptr % q.cap
}

func (q *Queue) writeMeta(pos uint64, meta int32) {
	// 4 byte
	*(*int32)(unsafe.Pointer(&q.mem[pos])) = meta
}

func (q *Queue) readMeta(pos uint64) (meta int32) {
	// 4 byte
	return *(*int32)(unsafe.Pointer(&q.mem[pos]))
}

// len(buf) must less than 2^31
func (q *Queue) Push(buf []byte) error {
	if q.IsFull(uint64(len(buf))) {
		return ErrFull
	}

	var writePtr = q.writePtr
	var writePtrNext = writePtr + ElementMetaSize + uint64(len(buf))

	// 1. skip tail mem that too short to write by one time
	if q.cap-q.pos(writePtr) < uint64(len(buf))+ElementMetaSize {
		// skip the range mem[pos(writePtr):q.cap] in mem
		var skip int32 = int32(q.cap - q.pos(writePtr))

		//println("skip: ", -skip)
		q.writeMeta(q.pos(writePtr), -skip)

		// update index
		writePtr += uint64(skip)
		writePtrNext = writePtr + ElementMetaSize + uint64(len(buf))
	}

	// 2.1 write meta
	q.writeMeta(q.pos(writePtr), int32(len(buf)))
	// 2.2 copy buf data
	copy(q.mem[q.pos(writePtr)+ElementMetaSize:q.pos(writePtr)+ElementMetaSize+uint64(len(buf))], buf)

	// 3. update index
	// use store act like memory barrier
	atomic.StoreUint64(&q.writePtr, writePtrNext)
	return nil
}

// arg dst must not nil
func (q *Queue) Pop(dst []byte) ([]byte, error) {
	if q.IsEmpty() {
		return nil, ErrEmpty
	}

	var readingPtr uint64 = q.readPtr

	// 1.1 read meta
	var meta int32 = q.readMeta(q.pos(readingPtr))

	// check if this skip is for this `pop`
	if meta < 0 {
		var skip int32 = -meta
		// skip the empty padding
		readingPtr += uint64(skip)

		//println("skip read: ", skip)

		// read the real meta
		meta = q.readMeta(q.pos(readingPtr))
		if meta < 0 {
			panic("unreachable")
		}
	}
	readingPtr += ElementMetaSize
	var dataSize uint64 = uint64(meta)

	// 1.2 copy data
	dst = append(dst, q.mem[q.pos(readingPtr):q.pos(readingPtr)+dataSize]...)

	// 2 update index
	var readPtrNext uint64 = readingPtr + dataSize
	// use store act like memory barrier
	atomic.StoreUint64(&q.readPtr, readPtrNext)

	return dst, nil
}
