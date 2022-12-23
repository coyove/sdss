package bitmap

import (
	"fmt"
	"time"

	"github.com/coyove/sdss/contrib/clock"
)

var ErrBitmapFull = fmt.Errorf("bitmap full (%d)", Capcity)

type aggTask struct {
	key    Key
	values []uint64
	out    chan error
}

type SaveAggregator struct {
	cb        func(*Range) error
	tasks     chan *aggTask
	workerOut chan bool
	current   *Range

	survey struct {
		c, r int
	}
}

func (r *Range) AggregateSaves(callback func(*Range) error) *SaveAggregator {
	fts := &SaveAggregator{}
	fts.tasks = make(chan *aggTask, 1000)
	fts.workerOut = make(chan bool, 1)
	fts.cb = callback
	fts.current = r

	go func() {
		for fts.worker() {
		}
		fts.workerOut <- true
	}()
	return fts
}

func (fts *SaveAggregator) Range() *Range {
	return fts.current
}

func (fts *SaveAggregator) Close() {
	close(fts.tasks)
	<-fts.workerOut
}

func (fts *SaveAggregator) worker() bool {
	start := clock.UnixNano()
	var tasks []*aggTask

MORE:
	to := time.Millisecond * 100
	if len(tasks) > 90 {
		to = time.Millisecond * 10
	} else {
		to = time.Millisecond * time.Duration(100-len(tasks))
	}

	select {
	case t, ok := <-fts.tasks:
		if !ok {
			if len(tasks) == 0 {
				return false
			}
		} else {
			tasks = append(tasks, t)
			if clock.UnixNano()-start < 1e9 {
				goto MORE
			}
		}
	case <-time.After(to):
	}

	if len(tasks) == 0 {
		return true
	}

	fts.survey.c += len(tasks)
	fts.survey.r += 1

	for i, t := range tasks {
		if !fts.current.Add(t.key, t.values) {
			for j := i; j < len(tasks); j++ {
				tasks[j].out <- ErrBitmapFull
			}
			tasks = tasks[:i]
			break
		}
	}

	err := fts.cb(fts.current)
	for _, t := range tasks {
		t.out <- err
	}
	return true
}

func (fts *SaveAggregator) AddAsync(key Key, values []uint64) chan error {
	t := &aggTask{
		key:    key,
		values: values,
		out:    make(chan error, 1),
	}
	fts.tasks <- t
	return t.out
}

func (fts *SaveAggregator) Add(key Key, values []uint64) error {
	return <-fts.AddAsync(key, values)
}

func (fts *SaveAggregator) Metrics() float64 {
	return float64(fts.survey.c) / float64(fts.survey.r)
}
