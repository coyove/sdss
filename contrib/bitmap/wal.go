package bitmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/coyove/sdss/contrib/clock"
	"github.com/tidwall/wal"
)

type WAL struct {
	mu    sync.Mutex
	popmu sync.Mutex
	path  string
	log   *wal.Log
	first uint64
	index uint64
}

type WALEntry struct {
	Namespace string
	Key       uint64
	Timestamp int64
	Values    []uint32
}

func Open(path string) (*WAL, error) {
	log, err := wal.Open(path, nil)
	if err != nil {
		return nil, err
	}
	idx, err := log.LastIndex()
	if err != nil {
		return nil, err
	}
	first, err := log.FirstIndex()
	if err != nil {
		return nil, err
	}
	w := &WAL{
		log:   log,
		index: idx,
		first: first,
		path:  path,
	}
	return w, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.log.Sync(); err != nil {
		return err
	}
	return w.log.Close()
}

func (w *WAL) Add(ns string, key uint64, v []uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	h := crc32.NewIEEE()
	buf := &bytes.Buffer{}

	x := io.MultiWriter(buf, h)
	binary.Write(x, binary.BigEndian, uint32(len(ns)))
	binary.Write(x, binary.BigEndian, []byte(ns))
	binary.Write(x, binary.BigEndian, key)
	binary.Write(x, binary.BigEndian, clock.UnixDeci())
	binary.Write(x, binary.BigEndian, uint32(len(v)))
	binary.Write(x, binary.BigEndian, v)
	binary.Write(x, binary.BigEndian, h.Sum32())

	if err := w.log.Write(w.index+1, buf.Bytes()); err != nil {
		return err
	}
	w.index++
	return nil
}

func walUnmarshal(buf []byte) (WALEntry, error) {
	h := crc32.NewIEEE()
	rd := io.TeeReader(bytes.NewReader(buf), h)

	res := WALEntry{}
	var nsLen uint32
	if err := binary.Read(rd, binary.BigEndian, &nsLen); err != nil {
		return res, fmt.Errorf("WAL: invalid entry: %v", err)
	}
	ns := make([]byte, nsLen)
	if err := binary.Read(rd, binary.BigEndian, ns); err != nil {
		return res, fmt.Errorf("WAL: invalid entry: %v", err)
	}
	res.Namespace = string(ns)
	if err := binary.Read(rd, binary.BigEndian, &res.Key); err != nil {
		return res, fmt.Errorf("WAL: invalid entry: %v", err)
	}
	if err := binary.Read(rd, binary.BigEndian, &res.Timestamp); err != nil {
		return res, fmt.Errorf("WAL: invalid entry: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &nsLen); err != nil {
		return res, fmt.Errorf("WAL: invalid entry: %v", err)
	}
	res.Values = make([]uint32, nsLen)
	if err := binary.Read(rd, binary.BigEndian, res.Values); err != nil {
		return res, fmt.Errorf("WAL: invalid entry: %v", err)
	}

	checksum := h.Sum32()
	var verify uint32
	if err := binary.Read(rd, binary.BigEndian, &verify); err != nil {
		return res, fmt.Errorf("WAL: invalid entry: %v", err)
	}
	if checksum != verify {
		return res, fmt.Errorf("WAL: invalid entry: %v and %v", verify, checksum)
	}
	return res, nil
}

func (w *WAL) First() uint64 { return w.first }

func (w *WAL) Last() uint64 { return w.index }

func (w *WAL) Get(index uint64) (WALEntry, error) {
	buf, _ := w.log.Read(index)
	return walUnmarshal(buf)
}

func (w *WAL) Pop(n int, f func(map[string][]WALEntry) error) error {
	w.popmu.Lock()
	defer w.popmu.Unlock()

	start := w.first
	end := start + uint64(n) - 1
	if end > w.index {
		end = w.index
	}

	data := map[string][]WALEntry{}
	for i := start; i <= end; i++ {
		buf, err := w.log.Read(i)
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		x, err := walUnmarshal(buf)
		data[x.Namespace] = append(data[x.Namespace], x)
	}

	if err := f(data); err != nil {
		return err
	}

	return w.log.TruncateFront(end + 1)
}

func (w *WAL) String() string {
	f, _ := w.log.FirstIndex()
	l, _ := w.log.LastIndex()
	return fmt.Sprintf("path: %s, index: %d-%d, log: %d-%d", w.path, w.first, w.index, f, l)
}
