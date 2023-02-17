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

	window time.Duration
}

func (r *Range) AggregateSaves(callback func(*Range) error) *SaveAggregator {
	fts := &SaveAggregator{}
	fts.tasks = make(chan *aggTask, 10000)
	fts.workerOut = make(chan bool, 1)
	fts.cb = callback
	fts.current = r
	fts.window = 100 * time.Millisecond

	go func() {
		for fts.worker() {
		}
		fts.workerOut <- true
	}()
	return fts
}

func (sa *SaveAggregator) SetWindow(w time.Duration) *SaveAggregator {
	sa.window = w
	return sa
}

func (sa *SaveAggregator) Range() *Range {
	return sa.current
}

func (sa *SaveAggregator) Close() {
	close(sa.tasks)
	<-sa.workerOut
}

func (sa *SaveAggregator) worker() bool {
	start := clock.UnixNano()
	tm := time.NewTimer(sa.window)

	var tasks []*aggTask

MORE:
	select {
	case t, ok := <-sa.tasks:
		if !ok {
			if len(tasks) == 0 {
				return false
			}
		} else {
			tasks = append(tasks, t)
			if clock.UnixNano()-start < sa.window.Nanoseconds() {
				goto MORE
			}
		}
	case <-tm.C:
	}

	tm.Stop()
	if len(tasks) == 0 {
		return true
	}

	sa.survey.c += len(tasks)
	sa.survey.r += 1
	if sa.survey.r > 100 {
		sa.survey.c = int(sa.Metrics())
		sa.survey.r = 1
	}

	for i, t := range tasks {
		if !sa.current.Add(t.key, t.values) {
			for j := i; j < len(tasks); j++ {
				tasks[j].out <- ErrBitmapFull
			}
			tasks = tasks[:i]
			break
		}
	}

	err := sa.cb(sa.current)
	for _, t := range tasks {
		t.out <- err
	}
	return true
}

func (sa *SaveAggregator) AddAsync(key Key, values []uint64) chan error {
	t := &aggTask{
		key:    key,
		values: values,
		out:    make(chan error, 1),
	}
	sa.tasks <- t
	return t.out
}

func (sa *SaveAggregator) Add(key Key, values []uint64) error {
	return <-sa.AddAsync(key, values)
}

func (sa *SaveAggregator) Metrics() float64 {
	return float64(sa.survey.c) / float64(sa.survey.r)
}
