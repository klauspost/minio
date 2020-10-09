/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
)

// localMetacacheMgr is the *local* manager for this peer.
// It should never be used directly since buckets are
// distributed deterministically.
// Therefore no cluster locks are required.
var localMetacacheMgr = &metacacheManager{
	buckets: make(map[string]*bucketMetacache),
}

type metacacheManager struct {
	mu      sync.RWMutex
	init    sync.Once
	buckets map[string]*bucketMetacache
}

// initManager will start async saving the cache.
func (m *metacacheManager) initManager() {
	go func() {
		// Start saver when object layer is ready.
		objAPI := newObjectLayerFn()
		for objAPI == nil {
			time.Sleep(time.Second)
			objAPI = newObjectLayerFn()
		}
		fmt.Println("init cleanup")

		t := time.NewTicker(time.Minute)
		var exit bool
		bg := context.Background()
		for !exit {
			select {
			case <-t.C:
			case <-GlobalContext.Done():
				exit = true
			}
			fmt.Println("starting cleanup")
			m.mu.RLock()
			for _, v := range m.buckets {
				if !exit {
					v.cleanup()
				}
				logger.LogIf(bg, v.save(bg))
			}
			m.mu.RUnlock()
		}
	}()
}

// getBucket will get a bucket metacache or load it from disk if needed.
func (m *metacacheManager) getBucket(ctx context.Context, bucket string) *bucketMetacache {
	m.init.Do(m.initManager)
	if isReservedOrInvalidBucket(bucket, false) {
		return nil
	}
	m.mu.RLock()
	b, ok := m.buckets[bucket]
	m.mu.RUnlock()
	if ok {
		return b
	}

	m.mu.Lock()
	// See if someone else fetched it while we waited for the lock.
	b, ok = m.buckets[bucket]
	if ok {
		m.mu.Unlock()
		return b
	}
	b, err := loadBucketMetaCache(context.Background(), bucket)
	if err == nil {
		m.buckets[bucket] = b
	}
	m.mu.Unlock()
	return b
}
