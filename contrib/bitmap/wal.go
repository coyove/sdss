package bitmap

// type WAL struct {
// 	mu        sync.Mutex
// 	flushmu   sync.Mutex
// 	path      string
// 	log       *wal.Log
// 	index     uint64
// 	name      string
// 	bitmap    *Day
// 	worker    *time.Ticker
// 	workerErr error
// }
//
// type WALEntry struct {
// 	Key       uint64
// 	Timestamp int64
// 	Values    []uint32
// }
//
// func OpenWAL(dir string, ns string, baseTime int64) (*WAL, error) {
// 	baseTime = baseTime / day * day
// 	name := fmt.Sprintf("%s_%d", ns, baseTime)
// 	path := filepath.Join(dir, name)
//
// 	log, err := wal.Open(path+".wal", nil)
// 	if err != nil {
// 		return nil, err
// 	}
// 	idx, err := log.LastIndex()
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	bm, err := Load(path)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if bm == nil {
// 		bm = New(baseTime, 2)
// 	}
//
// 	t := time.NewTicker(time.Second * 2)
// 	w := &WAL{
// 		log:    log,
// 		index:  idx,
// 		path:   path,
// 		worker: t,
// 		name:   name,
// 		bitmap: bm,
// 	}
// 	go func() {
// 		for range w.worker.C {
// 			if err := w.FlushData(); err != nil {
// 				w.workerErr = err
// 				break
// 			}
// 		}
// 	}()
// 	return w, nil
// }
//
// func (w *WAL) Close() error {
// 	w.mu.Lock()
// 	defer w.mu.Unlock()
// 	w.worker.Stop()
// 	if err := w.log.Sync(); err != nil {
// 		return err
// 	}
// 	if err := w.FlushData(); err != nil {
// 		return err
// 	}
// 	if err := w.log.Close(); err != nil {
// 		return err
// 	}
// 	return nil
// }
//
// func (w *WAL) Add(key uint64, v []uint32) error {
// 	w.mu.Lock()
// 	defer w.mu.Unlock()
// 	if w.workerErr != nil {
// 		return w.workerErr
// 	}
// 	ts := clock.UnixDeci()
// 	if ts/10/day*day != w.bitmap.BaseTime() {
// 		return fmt.Errorf("write to old WAL: %d and %d", ts/10, w.bitmap.baseTime/10)
// 	}
// 	return w.addWithTime(key, ts, v)
// }
//
// func (w *WAL) addWithTime(key uint64, ts int64, v []uint32) error {
// 	h := crc32.NewIEEE()
// 	buf := &bytes.Buffer{}
//
// 	x := io.MultiWriter(buf, h)
// 	binary.Write(x, binary.BigEndian, key)
// 	binary.Write(x, binary.BigEndian, ts)
// 	binary.Write(x, binary.BigEndian, uint32(len(v)))
// 	binary.Write(x, binary.BigEndian, v)
// 	binary.Write(x, binary.BigEndian, h.Sum32())
//
// 	if err := w.log.Write(w.index+1, buf.Bytes()); err != nil {
// 		return err
// 	}
// 	w.index++
// 	return nil
// }
//
// func walUnmarshal(buf []byte) (WALEntry, error) {
// 	h := crc32.NewIEEE()
// 	rd := io.TeeReader(bytes.NewReader(buf), h)
//
// 	res := WALEntry{}
// 	if err := binary.Read(rd, binary.BigEndian, &res.Key); err != nil {
// 		return res, fmt.Errorf("WAL: invalid entry: %v", err)
// 	}
// 	if err := binary.Read(rd, binary.BigEndian, &res.Timestamp); err != nil {
// 		return res, fmt.Errorf("WAL: invalid entry: %v", err)
// 	}
//
// 	var nsLen uint32
// 	if err := binary.Read(rd, binary.BigEndian, &nsLen); err != nil {
// 		return res, fmt.Errorf("WAL: invalid entry: %v", err)
// 	}
// 	res.Values = make([]uint32, nsLen)
// 	if err := binary.Read(rd, binary.BigEndian, res.Values); err != nil {
// 		return res, fmt.Errorf("WAL: invalid entry: %v", err)
// 	}
//
// 	checksum := h.Sum32()
// 	var verify uint32
// 	if err := binary.Read(rd, binary.BigEndian, &verify); err != nil {
// 		return res, fmt.Errorf("WAL: invalid entry: %v", err)
// 	}
// 	if checksum != verify {
// 		return res, fmt.Errorf("WAL: invalid entry: %v and %v", verify, checksum)
// 	}
// 	return res, nil
// }
//
// func (w *WAL) Last() uint64 { return w.index }
//
// func (w *WAL) Get(index uint64) (WALEntry, error) {
// 	buf, _ := w.log.Read(index)
// 	return walUnmarshal(buf)
// }
//
// func (w *WAL) FlushData() error {
// 	w.flushmu.Lock()
// 	defer w.flushmu.Unlock()
//
// 	start, err := w.log.FirstIndex()
// 	if err != nil {
// 		return err
// 	}
// 	end := w.index
//
// 	var data []WALEntry
// 	for i := start; i <= end; i++ {
// 		buf, err := w.log.Read(i)
// 		if err != nil {
// 			return err
// 		}
// 		x, err := walUnmarshal(buf)
// 		if err != nil {
// 			return err
// 		}
// 		data = append(data, x)
// 	}
//
// 	for _, e := range data {
// 		w.bitmap.addWithTime(e.Key, e.Timestamp, e.Values)
// 		fmt.Println("--->", e.Timestamp)
// 	}
// 	fmt.Println("fdlush", len(data))
// 	if _, err := w.bitmap.Save(w.path); err != nil {
// 		return err
// 	}
// 	return w.log.TruncateFront(end + 1)
// }
//
// func (w *WAL) String() string {
// 	f, _ := w.log.FirstIndex()
// 	l, _ := w.log.LastIndex()
// 	return fmt.Sprintf("path: %s, index: %d, log: %d-%d", w.path, w.index, f, l)
// }
