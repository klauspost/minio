// Copyright (c) 2015-2021 MinIO, Inc.
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

//go:generate msgp -file=$GOFILE -unexported

package cmd

import (
	"time"

	"github.com/minio/minio/internal/stats"
)

const (
	sizeLessThan1KiB = iota
	sizeLessThan1MiB
	sizeLessThan10MiB
	sizeLessThan100MiB
	sizeLessThan1GiB
	sizeGreaterThan1GiB
	// Add new entries here

	sizeLastElemMarker
)

// sizeToTag converts a size to a tag.
func sizeToTag(size int64) int {
	switch {
	case size < 1024:
		return sizeLessThan1KiB
	case size < 1024*1024:
		return sizeLessThan1MiB
	case size < 10*1024*1024:
		return sizeLessThan10MiB
	case size < 100*1024*1024:
		return sizeLessThan100MiB
	case size < 1024*1024*1024:
		return sizeLessThan1GiB
	default:
		return sizeGreaterThan1GiB
	}
}

func sizeTagToString(tag int) string {
	switch tag {
	case sizeLessThan1KiB:
		return "LESS_THAN_1_KiB"
	case sizeLessThan1MiB:
		return "LESS_THAN_1_MiB"
	case sizeLessThan10MiB:
		return "LESS_THAN_10_MiB"
	case sizeLessThan100MiB:
		return "LESS_THAN_100_MiB"
	case sizeLessThan1GiB:
		return "LESS_THAN_1_GiB"
	case sizeGreaterThan1GiB:
		return "GREATER_THAN_1_GiB"
	default:
		return "unknown"
	}
}

// LastMinuteHistogram keeps track of last minute sizes added.
type LastMinuteHistogram [sizeLastElemMarker]stats.LastMinuteLatency

// Merge safely merges two LastMinuteHistogram structures into one
func (l LastMinuteHistogram) Merge(o LastMinuteHistogram) (merged LastMinuteHistogram) {
	for i := range l {
		merged[i] = l[i].Merge(&o[i])
	}
	return merged
}

// Add latency t from object with the specified size.
func (l *LastMinuteHistogram) Add(size int64, t time.Duration) {
	l[sizeToTag(size)].Add(t)
}

// GetAvgData will return the average for each bucket from the last time minute.
// The number of objects is also included.
func (l *LastMinuteHistogram) GetAvgData() [sizeLastElemMarker]stats.AccElem {
	var res [sizeLastElemMarker]stats.AccElem
	for i, elem := range l[:] {
		res[i] = elem.GetTotal()
	}
	return res
}
