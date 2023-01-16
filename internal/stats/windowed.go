// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package stats

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v2"
)

// AccElem holds information for calculating an average value
type AccElem struct {
	Total int64
	Size  int64
	N     int64
}

// Add a duration to a single element.
func (a *AccElem) Add(dur time.Duration) {
	if dur < 0 {
		dur = 0
	}
	a.Total += int64(dur)
	a.N++
}

// AddSize a duration with size to a single element.
func (a *AccElem) AddSize(dur time.Duration, sz int64) {
	if dur < 0 {
		dur = 0
	}
	atomic.AddInt64(&a.Total, int64(dur))
	atomic.AddInt64(&a.Size, sz)
	atomic.AddInt64(&a.N, 1)
}

// Merge b into a.
// Only b is accessed atomically.
func (a *AccElem) Merge(b *AccElem) {
	a.N += atomic.LoadInt64(&b.N)
	a.Total += atomic.LoadInt64(&b.Total)
	a.Size += atomic.LoadInt64(&b.Size)
}

// AvgTime returns average time spent.
func (a *AccElem) AvgTime() time.Duration {
	n := atomic.LoadInt64(&a.N)
	total := atomic.LoadInt64(&a.Total)
	if n >= 1 && total > 0 {
		return time.Duration(total / n)
	}
	return 0
}

// AvgSize returns average size of operations.
func (a *AccElem) AvgSize() float64 {
	n := atomic.LoadInt64(&a.N)
	size := atomic.LoadInt64(&a.Size)
	if n >= 1 && size > 0 {
		return float64(size) / float64(n)
	}
	return 0
}

// AsTimedAction returns the element as a madmin.TimedAction.
func (a AccElem) AsTimedAction() madmin.TimedAction {
	return madmin.TimedAction{AccTime: uint64(a.Total), Count: uint64(a.N), Bytes: uint64(a.Size)}
}

// LastMinuteLatency keeps track of last minute latency.
type LastMinuteLatency struct {
	Totals   [60]AccElem
	LastSec  int64
	rotateMu sync.Mutex
}

// Merge data of o into l.
// Only 'o' will be using atomic reads.
func (l *LastMinuteLatency) Merge(o *LastMinuteLatency) {
	if l.LastSec > o.LastSec {
		o.forwardTo(l.LastSec)
	} else {
		l.forwardTo(o.LastSec)
	}

	for i := range l.Totals {
		l.Totals[i] = AccElem{
			Total: l.Totals[i].Total + atomic.LoadInt64(&o.Totals[i].Total),
			N:     l.Totals[i].N + atomic.LoadInt64(&o.Totals[i].N),
			Size:  l.Totals[i].Size + atomic.LoadInt64(&o.Totals[i].Size),
		}
	}
}

// Add  a new duration data
func (l *LastMinuteLatency) Add(t time.Duration) {
	sec := time.Now().Unix()
	l.Totals[l.forwardTo(sec)].Add(t)
	l.LastSec = sec
}

// AddSize a new duration with a size
func (l *LastMinuteLatency) AddSize(t time.Duration, sz int64) {
	sec := time.Now().Unix()
	l.Totals[l.forwardTo(sec)].AddSize(t, sz)
}

// GetTotal all recorded latencies of last minute into one.
func (l *LastMinuteLatency) GetTotal() AccElem {
	var res AccElem
	sec := time.Now().Unix()
	l.forwardTo(sec)
	for i := range l.Totals[:] {
		res.Merge(&l.Totals[i])
	}
	return res
}

// forwardTo time t, clearing any entries in between.
// Returns the index corresponding to t.
func (l *LastMinuteLatency) forwardTo(t int64) int {
	last := atomic.LoadInt64(&l.LastSec)
	if last >= t {
		return int(last % 60)
	}

	l.rotateMu.Lock()
	defer l.rotateMu.Unlock()
	last = l.LastSec
	if last >= t {
		return int(last % 60)
	}

	if t-last >= 60 {
		for i := range l.Totals[:] {
			atomic.StoreInt64(&l.Totals[i].Total, 0)
			atomic.StoreInt64(&l.Totals[i].N, 0)
			atomic.StoreInt64(&l.Totals[i].Size, 0)
		}
		return int(t % 60)
	}
	for last != t {
		// Clear next element.
		idx := (l.LastSec + 1) % 60
		atomic.StoreInt64(&l.Totals[idx].Total, 0)
		atomic.StoreInt64(&l.Totals[idx].N, 0)
		atomic.StoreInt64(&l.Totals[idx].Size, 0)
	}
	atomic.StoreInt64(&l.LastSec, last)
	return int(t % 60)
}
