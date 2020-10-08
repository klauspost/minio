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
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"

	"github.com/minio/minio/pkg/hash"

	"github.com/minio/minio/cmd/logger"
	"github.com/tinylib/msgp/msgp"
)

type scanStatus uint8

const (
	scanStateNone scanStatus = iota
	scanStateStarted
	scanStateSuccess
	scanStateError

	// Time in which the initiator of a scan must have reported back.
	metacacheMaxRunningAge = time.Minute
)

//go:generate msgp -file $GOFILE -unexported

// metacache contains a tracked cache entry.
type metacache struct {
	id           string     `msg:"id"`
	bucket       string     `msg:"b"`
	root         string     `msg:"root"`
	recursive    bool       `msg:"rec"`
	status       scanStatus `msg:"stat"`
	error        string     `msg:"err"`
	totalObjects uint64     `msg:"to"`
	started      time.Time  `msg:"st"`
	ended        time.Time  `msg:"end"`
	lastUpdate   time.Time  `msg:"u"`
	lastHandout  time.Time  `msg:"lh"`
	startedCycle uint64     `msg:"stc"`
	endedCycle   uint64     `msg:"endc"`
	dataVersion  uint8      `msg:"v"`
}

func init() {
	gob.Register(metacache{})
}

func (m *metacache) finished() bool {
	return !m.ended.IsZero()
}

func (m *metacache) worthKeeping(currentCycle uint64) bool {
	if m == nil {
		return false
	}
	cache := m
	switch {
	case !cache.finished() && time.Since(cache.lastUpdate) > metacacheMaxRunningAge:
		// Not finished and update for metacacheMaxRunningAge, discard it.
		return false
	case cache.finished() && cache.endedCycle > currentCycle:
		// Cycle is somehow bigger.
		return false
	case cache.finished() && currentCycle >= dataUsageUpdateDirCycles && cache.endedCycle < currentCycle-dataUsageUpdateDirCycles:
		// Cycle is too old to be valuable.
		return false
	case cache.status == scanStateError || cache.status == scanStateNone:
		// Remove failed listings
		return false
	}
	return true
}

// canBeReplacedBy.
// Both must pass the worthKeeping check.
func (m *metacache) canBeReplacedBy(other *metacache) bool {
	// If the other is older it can never replace.
	if other.started.Before(m.started) {
		return false
	}

	// Keep it around a bit longer.
	if time.Since(m.lastHandout) < time.Hour {
		return false
	}

	// Go through recursive combinations.
	switch {
	case !m.recursive && !other.recursive:
		// If both not recursive root must match.
		return m.root == other.root
	case m.recursive && !other.recursive:
		// A recursive can never be replaced by a non-recursive
		return false
	case !m.recursive && other.recursive:
		// If other is recursive it must contain this root
		return strings.HasPrefix(m.root, other.root)
	case m.recursive && other.recursive:
		// Similar if both are recursive
		return strings.HasPrefix(m.root, other.root)
	}
	fmt.Println("unreachable")
	return true
}

type bucketMetacache struct {
	// Name of bucket
	bucket string

	// caches indexed by id.
	caches map[string]metacache

	// Internal state
	mu      sync.RWMutex `msg:"-"`
	updated bool         `msg:"-"`
}

// loadBucketMetaCache will load the cache from the object layer.
// If the cache cannot be found a new one is created.
func loadBucketMetaCache(ctx context.Context, bucket string) (*bucketMetacache, error) {
	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		return nil, errServerNotInitialized
	}
	var meta bucketMetacache
	var decErr error
	var wg sync.WaitGroup
	wg.Add(1)

	r, w := io.Pipe()
	go func() {
		defer wg.Done()
		dec := s2DecPool.Get().(*s2.Reader)
		s2DecPool.Put(dec)
		dec.Reset(r)
		decErr = meta.DecodeMsg(msgp.NewReader(dec))
		dec.Reset(nil)
		s2DecPool.Put(dec)
	}()
	err := objAPI.GetObject(ctx, minioMetaBucket, pathJoin("buckets", bucket, ".metacache", "index.s2"), 0, -1, w, "", ObjectOptions{})
	logger.LogIf(ctx, w.CloseWithError(err))
	if err != nil {
		if err == errFileNotFound {
			err = nil
		} else {
			logger.LogIf(ctx, err)
		}
		return &bucketMetacache{bucket: bucket}, err
	}
	wg.Wait()
	if decErr != nil {
		logger.LogIf(ctx, decErr)
		return &bucketMetacache{bucket: bucket}, err
	}
	return &meta, nil
}

// save the bucket cache to the object storage.
func (b *bucketMetacache) save(ctx context.Context) error {
	objAPI := newObjectLayerWithoutSafeModeFn()
	if objAPI == nil {
		return errServerNotInitialized
	}

	// Keep lock while we marshal.
	// We need a write lock since we update 'updated'
	b.mu.Lock()
	if !b.updated {
		b.mu.Unlock()
		return nil
	}
	// Save as s2 compressed msgpack
	tmp := bytes.NewBuffer(make([]byte, 0, b.Msgsize()))
	enc := s2.NewWriter(tmp)
	err := msgp.Encode(enc, b)
	if err != nil {
		b.mu.Unlock()
		return err
	}
	err = enc.Close()
	if err != nil {
		b.mu.Unlock()
		return err
	}
	b.updated = false
	b.mu.Unlock()

	hr, err := hash.NewReader(tmp, int64(tmp.Len()), "", "", int64(tmp.Len()), false)
	if err != nil {
		return err
	}
	_, err = objAPI.PutObject(ctx, minioMetaBucket, pathJoin("buckets", b.bucket, ".metacache", "index.s2"), NewPutObjReader(hr, nil, nil), ObjectOptions{})
	logger.LogIf(ctx, err)
	return err
}

// findCache will attempt to find a matching cache for the provided options.
// If a cache with the same ID exists already it will be returned.
// If none can be found a new is created with the provided ID.
func (b *bucketMetacache) findCache(o listPathOptions) metacache {
	if o.Bucket != b.bucket {
		logger.Info("bucketMetacache.findCache: bucket does not match", o.Bucket, b.bucket)
		return metacache{}
	}
	thisRoot := baseDirFromPrefix(o.Prefix)

	// Grab a write lock, since we create one if we cannot find one.
	if o.Create {
		b.mu.Lock()
		defer b.mu.Unlock()
	} else {
		b.mu.RLock()
		defer b.mu.RUnlock()
	}

	// Check if exists already.
	if c, ok := b.caches[o.ID]; ok {
		return c
	}
	var best metacache
	for _, cached := range b.caches {
		if cached.status == scanStateError || cached.dataVersion != metacacheStreamVersion {
			continue
		}
		if cached.startedCycle < o.OldestCycle {
			continue
		}
		// Root of what we are looking for must at least have
		if !strings.HasPrefix(thisRoot, cached.root) {
			continue
		}
		// If the existing listing wasn't recursive root must match.
		if !cached.recursive && thisRoot != cached.root {
			continue
		}
		if o.Recursive && !cached.recursive {
			// If this is recursive the cached listing must be as well.
			continue
		}
		if o.Separator != slashSeparator && !cached.recursive {
			// Non slash separator requires recursive.
			continue
		}
		if cached.ended.IsZero() && time.Since(cached.lastUpdate) > metacacheMaxRunningAge {
			// Abandoned
			continue
		}
		if !cached.ended.IsZero() && cached.endedCycle <= o.OldestCycle {
			// If scan has ended the oldest requested must be less.
			continue
		}
		if cached.started.Before(best.started) {
			// If we already have a newer, keep that.
			continue
		}
	}
	if !best.started.IsZero() {
		// TODO: Update handout time.
		return best
	}
	if !o.Create {
		return metacache{
			id:     o.ID,
			status: scanStateNone,
		}
	}

	// Create new and add.
	best = metacache{
		id:           o.ID,
		bucket:       o.Bucket,
		root:         thisRoot,
		recursive:    o.Recursive,
		status:       scanStateStarted,
		error:        "",
		totalObjects: 0,
		started:      UTCNow(),
		lastHandout:  UTCNow(),
		lastUpdate:   UTCNow(),
		ended:        time.Time{},
		startedCycle: o.CurrentCycle,
		endedCycle:   0,
		dataVersion:  metacacheStreamVersion,
	}
	b.caches[o.ID] = best
	return best
}

// cleanup removes redundant and outdated entries.
func (b *bucketMetacache) cleanup() {
	// Entries to remove.
	remove := make(map[string]struct{})
	currentCycle := intDataUpdateTracker.current()

	b.mu.RLock()
	for id, cache := range b.caches {
		if !cache.worthKeeping(currentCycle) {
			remove[id] = struct{}{}
		}
	}

	// Check all non-deleted against eachother.
	// O(n*n), but should still be rather quick.
	for id, cache := range b.caches {
		if _, ok := remove[id]; ok {
			continue
		}
		for _, cache2 := range b.caches {
			if cache.canBeReplacedBy(&cache2) {
				remove[id] = struct{}{}
				break
			}
		}
	}

	b.mu.RUnlock()
	for id := range remove {
		b.deleteCache(id)
	}
}

// updateCache will update a cache by id.
// If the cache cannot be found nil is returned.
// The bucket cache will be locked until the done .
func (b *bucketMetacache) updateCache(id string) (cache *metacache, done func()) {
	b.mu.Lock()
	c, ok := b.caches[id]
	if !ok {
		b.mu.Unlock()
		return nil, func() {}
	}
	return &c, func() {
		c.lastUpdate = UTCNow()
		b.caches[id] = c
		b.mu.Unlock()
	}
}

// getCache will return a clone of
func (b *bucketMetacache) getCache(id string) *metacache {
	b.mu.RLock()
	c, ok := b.caches[id]
	b.mu.RUnlock()
	if !ok {
		return nil
	}
	return &c
}

func (b *bucketMetacache) deleteCache(id string) {
	b.mu.Lock()
	c, ok := b.caches[id]
	if ok {
		delete(b.caches, id)
		b.updated = true
	}
	b.mu.Unlock()
	if ok {
		ctx := context.Background()
		objAPI := newObjectLayerWithoutSafeModeFn()
		if objAPI == nil {
			logger.LogIf(ctx, errors.New("bucketMetacache: no object layer"))
			return
		}
		ez, ok := objAPI.(*erasureZones)
		if !ok {
			logger.LogIf(ctx, errors.New("bucketMetacache: expected objAPI to be *erasureZones"))
			return
		}
		logger.LogIf(ctx, ez.deleteAll(ctx, minioMetaBucket, metacachePrefixForID(c.bucket, c.id)))
	}
}

func baseDirFromPrefix(prefix string) string {
	b := path.Dir(prefix)
	if b == "." || b == "./" {
		b = ""
	}
	if len(b) > 0 && !strings.HasSuffix(b, slashSeparator) {
		b += slashSeparator
	}
	return b
}
