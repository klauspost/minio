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
	"errors"
	"fmt"
	"runtime/debug"
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
	// Start saver when object layer is ready.
	go func() {
		objAPI := newObjectLayerFn()
		for objAPI == nil {
			time.Sleep(time.Second)
			objAPI = newObjectLayerFn()
		}

		t := time.NewTicker(time.Minute)
		var exit bool
		bg := context.Background()
		for !exit {
			select {
			case <-t.C:
			case <-GlobalContext.Done():
				exit = true
			}
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
	m.mu.RLock()
	b, ok := m.buckets[bucket]
	m.mu.RUnlock()
	if ok {
		if b.bucket != bucket {
			logger.Info("getBucket: cached bucket %s does not match this bucket %s", b.bucket, bucket)
			debug.PrintStack()
		}
		return b
	}

	m.mu.Lock()
	// See if someone else fetched it while we waited for the lock.
	b, ok = m.buckets[bucket]
	if ok {
		m.mu.Unlock()
		if b.bucket != bucket {
			logger.Info("getBucket: newly cached bucket %s does not match this bucket %s", b.bucket, bucket)
			debug.PrintStack()
		}
		return b
	}
	// Load global context, so canceling doesn't abort it.
	b, err := loadBucketMetaCache(GlobalContext, bucket)
	if err == nil {
		if b.bucket != bucket {
			logger.Info("getBucket: loaded bucket %s does not match this bucket %s", b.bucket, bucket)
			debug.PrintStack()
		}
		m.buckets[bucket] = b
	}
	m.mu.Unlock()
	return b
}

// checkMetacacheState should be used if data is not updating.
// Should only be called if a failure occurred.
func (o listPathOptions) checkMetacacheState(ctx context.Context) error {
	// We operate on a copy...
	o.Create = false
	rpc := globalNotificationSys.restClientFromHash(o.Bucket)
	var cache metacache
	if rpc == nil {
		// Local
		cache = localMetacacheMgr.getBucket(ctx, o.Bucket).findCache(o)
	} else {
		c, err := rpc.GetMetacacheListing(ctx, o)
		if err != nil {
			return err
		}
		cache = *c
	}
	if cache.status == scanStateNone {
		return errFileNotFound
	}
	if cache.status == scanStateSuccess {
		if time.Since(cache.lastUpdate) > 10*time.Second {
			return fmt.Errorf("timeout: Finished and data not available after 10 seconds")
		}
		return nil
	}
	if cache.error != "" {
		return errors.New(cache.error)
	}
	if cache.status == scanStateStarted {
		if time.Since(cache.lastUpdate) > metacacheMaxRunningAge {
			return errors.New("cache listing not updating")
		}
	}
	return nil
}