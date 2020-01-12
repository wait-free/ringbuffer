package ringbuffer

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueueReadWrite(t *testing.T) {
	var q = NewQueue(111)

	for i := 0; i < 256; i++ {
		var input = []byte{1, 2, 3, 4}
		q.Push(input)

		var output []byte
		var err error
		output, err = q.Pop(output)
		assert.Nil(t, err)
		assert.Equal(t, input, output)
	}
}

func TestQueueZeroTest(t *testing.T) {
	var q = NewQueue(111)

	for i := 0; i < 256; i++ {
		var input = []byte{}
		q.Push(input)

		var output = []byte{}
		var err error
		output, err = q.Pop(output)
		assert.Nil(t, err)
		assert.Equal(t, input, output)
	}
}

func TestQueueWriteWithPadding(t *testing.T) {
	var q = NewQueue(63)

	// prepare
	for i := 0; i < 7; i++ {
		err := q.Push([]byte{byte(i), 2, 3, 4})
		assert.Nil(t, err)

		var buf []byte = make([]byte, 0, 4)
		buf, err = q.Pop(buf)
		assert.Nil(t, err)
	}

	assert.True(t, true, q.IsEmpty())

	var input = []byte{1, 2, 3, 4, 5, 6}
	q.Push(input)

	var output []byte
	var err error
	output, err = q.Pop(output)
	assert.Nil(t, err)
	assert.Equal(t, input, output)
}

func TestQueue_IsFull(t *testing.T) {
	var q = NewQueue(64)
	q.readPtr = 0
	q.writePtr = 0

	assert.False(t, q.IsFull(60))
	assert.True(t, q.IsFull(61))

	// case: readPtr < writePtr
	q.readPtr = 24
	q.writePtr = 48

	// header full
	assert.False(t, q.IsFull(20))
	assert.True(t, q.IsFull(21))

	// case: readPtr < writePtr
	q.readPtr = 12
	q.writePtr = 36

	// header full
	assert.False(t, q.IsFull(24))
	assert.True(t, q.IsFull(25))

	// case: writePtr < readPtr
	q.writePtr = 80
	q.readPtr = 48

	assert.False(t, q.IsFull(28))
	assert.True(t, q.IsFull(29))

}

func TestQueueWriteRandomPos(t *testing.T) {

	random, _ := os.Open("/dev/random")
	for i := 0; i < 256; i++ {
		var q = NewQueue(136)
		q.readPtr = uint64(i)
		q.writePtr = uint64(i)

		size := 64
		input := make([]byte, size)
		n, _ := random.Read(input)
		assert.Equal(t, int(size), n)

		//println("begin push:", len(input))

		err := q.Push(input)
		assert.Nil(t, err)
		if err != nil {
			panic(err)
		}

		var output []byte = make([]byte, 0, 64)
		output, err = q.Pop(output)
		assert.Nil(t, err)
		if !bytes.Equal(input, output) {
			panic("not equal")
		}
		//fmt.Println("input: ", input)
		//fmt.Println("outpu: ", output)
		assert.True(t, true, q.IsEmpty())
	}

}

func TestQueueWriteRandom(t *testing.T) {
	var q = NewQueue(256)

	random, _ := os.Open("/dev/random")
	for i := 0; i < 1000000; i++ {
		size := rand.Uint32() % 64
		input := make([]byte, size)
		n, _ := random.Read(input)
		assert.Equal(t, int(size), n)

		////println("begin push:", len(input))

		err := q.Push(input)
		assert.Nil(t, err)

		var output []byte
		output, err = q.Pop(output)
		assert.Nil(t, err)
		if !bytes.Equal(input, output) {
			panic("not equal")
		}
		////fmt.Println("input: ", input)
		////fmt.Println("outpu: ", output)
		assert.True(t, true, q.IsEmpty())
	}
}

func TestQueueReadWriteConsistenceWithContention(b *testing.T) {
	var input []byte
	var output []byte
	random, _ := os.Open("/dev/random")
	size := (1 << 29)
	input = make([]byte, size)
	output = make([]byte, size)
	n, _ := random.Read(input)
	assert.Equal(b, int(size), n)

	// make random inputs
	var inputs [][]byte
	var outputs [][]byte

	for start := 0; start < len(input); {
		size := int(rand.Uint32() % 64)
		end := start + size

		if end > len(input) {
			end = len(input)
		}

		inputs = append(inputs, input[start:end])
		outputs = append(outputs, output[start:end])

		start = end
	}
	println("init finish")

	var q = NewQueue(512 * 1024)

	var widx, ridx int

	var wg sync.WaitGroup
	wg.Add(2)

	// input wit go routine
	var writeRetryCnt int
	go func() {
		for widx = 0; widx < len(inputs); widx++ {
			msg := inputs[widx]
			for q.Push(msg) == ErrFull {
				// do not yields, just run msg parallel
				//runtime.Gosched()
				writeRetryCnt++
				////fmt.Println("widx: ", widx, "fullCnt:", fullCnt, ", len: ", len(msg))
			}
			////fmt.Println("msg: fullCnt: ", fullCnt, ", msg: ", msg)

		}
		wg.Done()
	}()

	// output with main routine
	var readRetryCnt int
	go func() {
		for ridx = 0; ridx < len(outputs); ridx++ {
			var output = []byte{}
			var err error
			output, err = q.Pop(output)
			for err == ErrEmpty {
				// do not yields, just run in parallel
				//runtime.Gosched()
				////fmt.Println("ridx: ", ridx, "emptyCnt:", emptyCnt)
				readRetryCnt++
				output, err = q.Pop(output)
			}
			assert.Equal(b, len(outputs[ridx]), len(output))

			copy(outputs[ridx], output)
			////fmt.Println("output: emptyCnt: ", emptyCnt, ", out: ", *output)
			assert.Equal(b, nil, err)
		}
		wg.Done()
	}()

	wg.Wait()

	fmt.Println("input retry count: ", writeRetryCnt)
	fmt.Println("output retry count: ", readRetryCnt)

	// check
	assert.True(b, bytes.Equal(input, output))

	for i := 0; i < len(inputs); i++ {
		if !bytes.Equal(inputs[i], outputs[i]) {
			panic("not equal")
		}
		assert.True(b, bytes.Equal(inputs[i], outputs[i]))
	}
}

// 读写交错, 没有竞争情况下的性能测试
func BenchmarkQueueReadWriteNoContention(b *testing.B) {
	var q = NewQueue(256)

	//println("N:", b.N)

	// make random inputs
	var inputs [][]byte
	var outputs [][]byte
	random, _ := os.Open("/dev/random")
	for i := 0; i < 1000; i++ {
		size := rand.Uint32() % 64
		buf := make([]byte, size)
		n, _ := random.Read(buf)
		assert.Equal(b, int(size), n)

		inputs = append(inputs, buf)
		outputs = append(outputs, make([]byte, 0, len(buf)))
	}
	//println("init finish")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		input := inputs[i%1000]
		if len(input) > 0 {
			input[0] = byte(i) // randomize
		}
		output := outputs[i%1000]

		err := q.Push(input)
		assert.Nil(b, err)

		output, err = q.Pop(output)
		assert.Nil(b, err)
		if !bytes.Equal(input, output) {
			panic("not equal")
		}
		////fmt.Println("inputs: ", inputs)
		////fmt.Println("outpu: ", outputs)
		assert.True(b, true, q.IsEmpty())
	}
	b.StopTimer()

	//println("finish")
}

func BenchmarkQueueReadWriteContention(b *testing.B) {
	println("N:", b.N)

	var inputSeds [][]byte
	random, _ := os.Open("/dev/random")
	for i := 0; i < 10000; i++ {
		size := rand.Uint32() % 128
		//size := 1
		buf := make([]byte, size)
		n, _ := random.Read(buf)
		assert.Equal(b, int(size), n)

		inputSeds = append(inputSeds, buf)
	}

	// make random inputs
	var inputs [][]byte
	var outputs [][]byte

	for i := 0; i < b.N; i++ {
		rnd := rand.Int() % len(inputSeds)
		//size := 1
		buf := inputSeds[rnd]
		if len(buf) > 0 {
			buf[0] = byte(i) // randomize
		}

		inputs = append(inputs, buf)
		outputs = append(outputs, make([]byte, 0, len(buf)))
	}
	println("init finish")

	b.ResetTimer()

	var q = NewQueue(1024 * 1024 * 10)

	var widx, ridx int

	var wg sync.WaitGroup
	wg.Add(2)

	// input wit go routine
	var writeRetryCnt int
	go func() {
		for widx = 0; widx < b.N; widx++ {
			input := inputs[widx]
			for q.Push(input) == ErrFull {
				// do not yields, just run in parallel
				//runtime.Gosched()
				writeRetryCnt++
				////fmt.Println("widx: ", widx, "fullCnt:", fullCnt, ", len: ", len(input))
			}
			////fmt.Println("input: fullCnt: ", fullCnt, ", in: ", input)

		}
		wg.Done()
	}()

	// output with main routine
	var readRetryCnt int
	go func() {
		for ridx = 0; ridx < b.N; ridx++ {
			var output = &outputs[ridx]
			var err error
			*output, err = q.Pop(*output)
			for err == ErrEmpty {
				// do not yields, just run in parallel
				//runtime.Gosched()
				////fmt.Println("ridx: ", ridx, "emptyCnt:", emptyCnt)
				readRetryCnt++
				*output, err = q.Pop(*output)
			}
			////fmt.Println("output: emptyCnt: ", emptyCnt, ", out: ", *output)
			assert.Equal(b, nil, err)

		}
		wg.Done()
	}()

	wg.Wait()
	b.ResetTimer()

	fmt.Println("input retry count: ", writeRetryCnt)
	fmt.Println("output retry count: ", readRetryCnt)

	// check
	for i := 0; i < b.N; i++ {
		if !bytes.Equal(inputs[i], outputs[i]) {
			panic("not equal")
		}
		assert.True(b, bytes.Equal(inputs[i], outputs[i]))
	}
}
