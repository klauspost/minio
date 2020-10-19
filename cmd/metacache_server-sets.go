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
	"io"
	"path"
	"sync"

	"github.com/minio/minio/cmd/logger"
)

// listPath will return the requested entries.
// If no more entries are in the listing io.EOF is returned,
// otherwise nil or an unexpected error is returned.
func (z *erasureServerSets) listPath(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (entries metaCacheEntriesSorted, err error) {
	if err := checkListObjsArgs(ctx, bucket, prefix, marker, z); err != nil {
		return entries, err
	}

	// Marker is set validate pre-condition.
	if marker != "" && prefix != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !HasPrefix(marker, prefix) {
			return entries, io.EOF
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return entries, io.EOF
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == SlashSeparator && prefix == SlashSeparator {
		return entries, io.EOF
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	// If delimiter is slashSeparator we must return directories of the non-recursive scan.
	withDirs := delimiter == slashSeparator
	if delimiter == slashSeparator || delimiter == "" {
		recursive = delimiter != slashSeparator
		delimiter = slashSeparator
	}

	// Decode and get the optional list id from the marker.
	marker, listID := parseMarker(marker)
	createNew := listID == ""
	if listID == "" {
		listID = mustGetUUID()
	}

	// Convert input to collected options.
	opts := listPathOptions{
		ID:                 listID,
		Bucket:             bucket,
		BaseDir:            baseDirFromPrefix(prefix),
		Prefix:             prefix,
		Marker:             marker,
		Limit:              maxKeys,
		IncludeDirectories: withDirs,
		InclDeleted:        true,
		Recursive:          recursive,
		Separator:          delimiter,
		Create:             createNew,
	}
	var cache metacache
	// If we don't have a list id we must ask the server if it has a cache or create a new.
	if createNew {
		opts.CurrentCycle = intDataUpdateTracker.current()
		opts.OldestCycle = globalNotificationSys.findEarliestCleanBloomFilter(ctx, path.Join(opts.Bucket, opts.BaseDir))
		var cache metacache
		rpc := globalNotificationSys.restClientFromHash(bucket)
		if rpc == nil {
			// Local
			cache = localMetacacheMgr.getBucket(ctx, bucket).findCache(opts)
		} else {
			c, err := rpc.GetMetacacheListing(ctx, opts)
			if err != nil {
				return entries, err
			}
			cache = *c
		}
		if cache.fileNotFound {
			return entries, errFileNotFound
		}
		// Only create if we created a new.
		opts.Create = opts.ID == cache.id
		opts.ID = cache.id
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	var errs []error
	allAtEOF := true
	asked := 0
	mu.Lock()
	// Ask all sets and merge entries.
	for _, zone := range z.serverSets {
		for _, set := range zone.sets {
			wg.Add(1)
			asked++
			go func(i int, set *erasureObjects) {
				defer wg.Done()
				e, err := set.listPath(ctx, opts)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					allAtEOF = false
				}
				errs[i] = err
				entries.merge(e, -1)

				// Resolve non-trivial conflicts
				entries.deduplicate(func(existing, other *metaCacheEntry) (replace bool) {
					if existing.isDir() {
						return false
					}
					eFIV, err := existing.fileInfo(bucket)
					if err != nil {
						return true
					}
					oFIV, err := existing.fileInfo(bucket)
					if err != nil {
						return false
					}
					return oFIV.ModTime.After(eFIV.ModTime)
				})
				if entries.len() > maxKeys {
					allAtEOF = false
					entries.truncate(maxKeys)
				}
			}(len(errs), set)
			errs = append(errs, nil)
		}
	}
	mu.Unlock()
	wg.Wait()

	if isAllNotFound(errs) {
		// All sets returned not found.
		// Update master cache with that information.
		cache.status = scanStateSuccess
		cache.fileNotFound = true
		client := globalNotificationSys.restClientFromHash(opts.Bucket)
		if client == nil {
			cache, err = localMetacacheMgr.getBucket(GlobalContext, opts.Bucket).updateCacheEntry(cache)
		} else {
			cache, err = client.UpdateMetacacheListing(context.Background(), cache)
		}
		logger.LogIf(ctx, err)
		return entries, errFileNotFound
	}

	for _, err := range errs {
		if err == nil {
			allAtEOF = false
			continue
		}
		if err == io.EOF {
			continue
		}
		logger.LogIf(ctx, err)
		return entries, err
	}
	truncated := entries.len() > maxKeys || !allAtEOF
	entries.truncate(maxKeys)
	entries.listID = opts.ID
	if !truncated {
		return entries, io.EOF
	}
	return entries, nil
}
