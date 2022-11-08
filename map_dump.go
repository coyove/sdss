package sdss

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
	"unsafe"
)

func readBytes(r io.Reader, size int) ([]byte, error) {
	res := make([]byte, size)
	_, err := io.ReadFull(r, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *Map) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	tmp := make([]byte, 14)
	if _, err := io.ReadFull(f, tmp); err != nil {
		return fmt.Errorf("load: failed to read header: %v", err)
	}

	if tmp[1] != 1 {
		return fmt.Errorf("load: invalid sync marker: %x", tmp[1])
	}

	m.count = int32(binary.BigEndian.Uint32(tmp[6:]))
	cap := int(binary.BigEndian.Uint32(tmp[10:]))

	keys, err := readBytes(f, cap*8)
	if err != nil {
		return fmt.Errorf("load: failed to read KEY bytes: %v", err)
	}
	*(*[3]int)(unsafe.Pointer(&m.keys)) = [3]int{*(*int)(unsafe.Pointer(&keys)), cap, cap}

	scores, err := readBytes(f, cap*8)
	if err != nil {
		return fmt.Errorf("load: failed to read SCORE bytes: %v", err)
	}
	*(*[3]int)(unsafe.Pointer(&m.scores)) = [3]int{*(*int)(unsafe.Pointer(&scores)), cap, cap}

	dists, err := readBytes(f, cap*4)
	if err != nil {
		return fmt.Errorf("load: failed to read DIST bytes: %v", err)
	}
	*(*[3]int)(unsafe.Pointer(&m.dists)) = [3]int{*(*int)(unsafe.Pointer(&dists)), cap, cap}

	return nil
}

func (m *Map) Save(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	tmp := make([]byte, 14)
	binary.BigEndian.PutUint32(tmp[2:], uint32(time.Now().Unix()))
	binary.BigEndian.PutUint32(tmp[6:], uint32(m.count))
	binary.BigEndian.PutUint32(tmp[10:], uint32(m.Cap()))
	if _, err := f.Write(tmp); err != nil {
		return err
	}

	*(*[3]int)(unsafe.Pointer(&tmp)) = [3]int{
		*(*int)(unsafe.Pointer(&m.keys)),
		int(m.Cap()) * 8,
		int(m.Cap()) * 8,
	}
	if _, err := f.Write(tmp); err != nil {
		return err
	}

	*(*[3]int)(unsafe.Pointer(&tmp)) = [3]int{
		*(*int)(unsafe.Pointer(&m.scores)),
		int(m.Cap()) * 8,
		int(m.Cap()) * 8,
	}
	if _, err := f.Write(tmp); err != nil {
		return err
	}

	*(*[3]int)(unsafe.Pointer(&tmp)) = [3]int{
		*(*int)(unsafe.Pointer(&m.dists)),
		int(m.Cap()) * 4,
		int(m.Cap()) * 4,
	}
	if _, err := f.Write(tmp); err != nil {
		return err
	}

	if _, err := f.WriteAt([]byte{1}, 1); err != nil {
		return err
	}
	return f.Sync()
}
