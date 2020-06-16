/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"testing"
	"time"
)

// Test if tree walker go-routine is removed from the pool after timeout
// and that is available in the pool before the timeout.
func TestMergeWalkPoolBasic(t *testing.T) {
	// Create a treeWalkPool
	tw := NewMergeWalkPool(1 * time.Second)

	// Create sample params
	params := listParams{
		bucket: "test-bucket",
	}

	endWalkCh := make(chan struct{})
	// Add a treeWalk to the pool
	tw.Set(params, []FileInfoCh{}, endWalkCh)

	// Wait for treeWalkPool timeout to happen
	<-time.After(2 * time.Second)
	if c1, _ := tw.Release(params); c1 != nil {
		t.Error("treeWalk go-routine must have been freed")
	}

	// Add the treeWalk back to the pool
	endWalkCh = make(chan struct{})
	tw.Set(params, []FileInfoCh{}, endWalkCh)

	// Release the treeWalk before timeout
	select {
	case <-time.After(1 * time.Second):
		break
	default:
		if c1, _ := tw.Release(params); c1 == nil {
			t.Error("treeWalk go-routine got freed before timeout")
		}
	}
}

// Test if multiple merge walkers for the same listParams are managed as expected by the pool.
func TestManyMergeWalksSameParam(t *testing.T) {
	// Create a treeWalkPool.
	tw := NewMergeWalkPool(5 * time.Second)

	// Create sample params.
	params := listParams{
		bucket: "test-bucket",
	}

	select {
	// This timeout is an upper-bound. This is started
	// before the first treeWalk go-routine's timeout period starts.
	case <-time.After(5 * time.Second):
		break
	default:
		// Create many treeWalk go-routines for the same params.
		for i := 0; i < treeWalkSameEntryLimit; i++ {
			endWalkCh := make(chan struct{})
			walkChs := make([]FileInfoCh, 0)
			tw.Set(params, walkChs, endWalkCh)
		}

		tw.Lock()
		if walks, ok := tw.pool[params]; ok {
			if len(walks) != treeWalkSameEntryLimit {
				t.Error("There aren't as many walks as were Set")
			}
		}
		tw.Unlock()
		for i := 0; i < treeWalkSameEntryLimit; i++ {
			tw.Lock()
			if walks, ok := tw.pool[params]; ok {
				// Before ith Release we should have n-i treeWalk go-routines.
				if treeWalkSameEntryLimit-i != len(walks) {
					t.Error("There aren't as many walks as were Set")
				}
			}
			tw.Unlock()
			tw.Release(params)
		}
	}

}

// Test if multiple merge walkers for the same listParams are managed as expected by the pool
// but that treeWalkSameEntryLimit is respected.
func TestManyMergeWalksSameParamPrune(t *testing.T) {
	// Create a treeWalkPool.
	tw := NewMergeWalkPool(5 * time.Second)

	// Create sample params.
	params := listParams{
		bucket: "test-bucket",
	}

	select {
	// This timeout is an upper-bound. This is started
	// before the first treeWalk go-routine's timeout period starts.
	case <-time.After(5 * time.Second):
		break
	default:
		// Create many treeWalk go-routines for the same params.
		for i := 0; i < treeWalkSameEntryLimit*4; i++ {
			endWalkCh := make(chan struct{})
			walkChs := make([]FileInfoCh, 0)
			tw.Set(params, walkChs, endWalkCh)
		}

		tw.Lock()
		if walks, ok := tw.pool[params]; ok {
			if len(walks) > treeWalkSameEntryLimit {
				t.Error("There aren't as many walks as were Set")
			}
		}
		tw.Unlock()
		for i := 0; i < treeWalkSameEntryLimit; i++ {
			tw.Lock()
			if walks, ok := tw.pool[params]; ok {
				// Before ith Release we should have n-i treeWalk go-routines.
				if treeWalkSameEntryLimit-i != len(walks) {
					t.Error("There aren't as many walks as were Set")
				}
			}
			tw.Unlock()
			tw.Release(params)
		}
	}

}
