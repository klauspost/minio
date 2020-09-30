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
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/cmd/config/storageclass"
	xhttp "github.com/minio/minio/cmd/http"

	"github.com/minio/minio/pkg/hash"

	"github.com/minio/minio/cmd/logger"
)

type listPathOptions struct {
	// ID of the listing.
	// This will be used to persist the list.
	ID string

	// Bucket of the listing.
	Bucket string

	// Directory inside the bucket.
	BaseDir string

	// Scan/return only content with prefix.
	Prefix string

	// Marker to resume listing.
	// The response will be the first entry AFTER this object name.
	Marker string

	// Limit the number of results.
	Limit int

	// InclDeleted will keep all entries where latest version is a delete marker.
	InclDeleted bool

	// Scan recursively.
	// If false only main directory will be scanned.
	// Should always be true if Separator is n SlashSeparator.
	Recursive bool

	// Separator to use.
	Separator string

	// Create indicates that the lister should not attempt to load an existing cache.
	Create bool
}

// gatherResults will collect all results on the input channel and filter results according to the options.
// Caller should close the channel when done.
// The returned function will return the results.
func (o *listPathOptions) gatherResults(in <-chan metaCacheEntry) func() metaCacheEntriesSorted {
	var results metaCacheEntriesSorted
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for entry := range in {
			if o.Limit > 0 && results.len() > o.Limit {
				continue
			}
			//fmt.Println("gather got:", entry.name)
			if o.Marker != "" && entry.name < o.Marker {
				//fmt.Println("pre marker")
				continue
			}
			if !strings.HasPrefix(entry.name, o.Prefix) {
				//fmt.Println("not in prefix")
				continue
			}
			if !o.Recursive && !entry.isInDir(o.Prefix, o.Separator) {
				//fmt.Println("not in dir", o.Prefix, o.Separator)
				continue
			}
			if !o.InclDeleted && entry.isObject() {
				if entry.isLatestDeletemarker() {
					//fmt.Println("latest delete")
					continue
				}
			}
			//fmt.Println("adding...")
			results.o = append(results.o, entry)
		}
	}()
	return func() metaCacheEntriesSorted {
		wg.Wait()
		return results
	}
}

// objectPath returns the object path of the cache.
func (o *listPathOptions) objectPath() string {
	return pathJoin("buckets", o.Bucket, ".metacache", o.ID+".s2")
}

// filter will apply the options and return the number of objects requested by the limit.
// Will return io.EOF if there are no more entries.
// The last entry can be used as a marker to resume the listing.
func (r *metacacheReader) filter(o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	// Forward to prefix, if any
	err = r.forwardTo(o.Prefix)
	if err != nil {
		return entries, err
	}
	fmt.Println("forwarded to ", o.Prefix)
	if o.Marker != "" {
		err = r.forwardTo(o.Marker)
		if err != nil {
			return entries, err
		}
	}
	// Filter
	if !o.Recursive {
		entries.o = make(metaCacheEntries, 0, o.Limit)
		err := r.readFn(func(entry metaCacheEntry) bool {
			if o.InclDeleted && entry.isObject() && entry.isLatestDeletemarker() {
				return entries.len() >= o.Limit
			}
			if entry.isInDir(o.Prefix, o.Separator) {
				entries.o = append(entries.o, entry)
			}
			return entries.len() >= o.Limit
		})
		if err == io.EOF {
			return entries, io.EOF
		}
		return entries, err
	}

	fmt.Println("returning", o.Limit, "results")
	defer fmt.Println("returned")

	// We should not need to filter more.
	return r.readN(o.Limit, o.InclDeleted)
}

// Will return io.EOF if continuing would not yield more results.
func (er erasureObjects) listPath(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	startTime := time.Now()
	//fmt.Println("set listing bucket:", o.Bucket, "basedir:", o.BaseDir)
	// See if we have the listing stored.
	// Not really a loop, we break out if we are unable read and need to fall back.
	for !o.Create {
		r, w := io.Pipe()
		go func() {
			err := er.getObject(ctx, minioMetaBucket, o.objectPath(), 0, -1, w, "", ObjectOptions{})
			w.CloseWithError(err)
		}()

		mr, err := newMetacacheReader(r)
		_, err2 := mr.peek()
		if err != nil || err2 != nil {
			r.Close()
			break
		}
		defer r.Close()
		defer mr.Close()
		return mr.filter(o)
	}

	// We need to ask disks.
	var disks []StorageAPI
	for _, d := range er.getLoadBalancedDisks() {
		if d == nil || !d.IsOnline() {
			continue
		}
		disks = append(disks, d)
	}

	const askDisks = 3
	if len(disks) < askDisks {
		err = InsufficientReadQuorum{}
		return
	}

	// Select askDisks random disks, 3 is ok.
	if len(disks) > askDisks {
		disks = disks[:askDisks]
	}
	var readers = make([]*metacacheReader, askDisks)

	for i := range disks {
		r, w := io.Pipe()
		d := disks[i]
		readers[i], err = newMetacacheReader(r)
		if err != nil {
			return entries, err
		}
		// Send request.
		go func() {
			err := d.WalkDir(ctx, WalkDirOptions{Bucket: o.Bucket, BaseDir: o.BaseDir, Recursive: o.Recursive || o.Separator != SlashSeparator}, w)
			w.CloseWithError(err)
			logger.LogIf(ctx, err)
		}()
	}

	// Create output for our results.
	cacheR, cacheW := io.Pipe()
	go func() {
		// Maybe we can use the real context...
		ctx := context.Background()
		r, err := hash.NewReader(cacheR, -1, "", "", -1, false)
		logger.LogIf(ctx, err)
		_, err = er.putObject(ctx, minioMetaBucket, o.objectPath(), NewPutObjReader(r, nil, nil), ObjectOptions{UserDefined: map[string]string{xhttp.AmzStorageClass: storageclass.RRS}})
		logger.LogIf(ctx, err)
	}()
	defer cacheW.Close()

	// Write to cache.
	cacheWriter := newMetacacheWriter(cacheW, 1<<20)
	defer cacheWriter.Close()
	cacheCh, err := cacheWriter.stream()
	if err != nil {
		logger.LogIf(ctx, err)
		return entries, err
	}

	// Create filter for results.
	filterCh := make(chan metaCacheEntry, 100)
	filteredResults := o.gatherResults(filterCh)
	closeChannels := func() {
		close(cacheCh)
		close(filterCh)
	}

	// Merge results from the readers.
	resolver := metadataResolutionParams{
		startTime: startTime,
		dirQuorum: askDisks - 1,
		bucket:    o.Bucket,
	}

	topEntries := make(metaCacheEntries, len(readers))
	for {
		if o.Limit > 0 && entries.len() == o.Limit {
			break
		}
		// Get the top entry from each
		var current metaCacheEntry
		var atEOF, agree int
		for i, r := range readers {
			topEntries[i].name = ""
			entry, err := r.peek()
			switch err {
			case io.EOF:
				atEOF++
				continue
			case nil:
			default:
				closeChannels()
				return entries, err
			}
			if entry.name == current.name || current.name == "" {
				topEntries[i] = entry
				if current.name == "" || bytes.Equal(current.metadata, entry.metadata) {
					agree++
					continue
				}
				current = entry
				continue
			}
			// We got different entries
			if entry.name > current.name {
				continue
			}
			// We got a new, better current.
			for i := range topEntries[:i] {
				topEntries[i] = metaCacheEntry{}
			}
			agree = 1
			current = entry
			topEntries[i] = entry
		}
		// Break if all at EOF.
		if atEOF == len(readers) {
			break
		}
		if agree == len(readers) {
			// Everybody agreed
			for _, r := range readers {
				r.skip(1)
			}
			cacheCh <- topEntries[0]
			filterCh <- topEntries[0]
			continue
		}

		// Results Disagree :-(
		entry, ok := topEntries.resolve(&resolver)
		if ok {
			cacheCh <- *entry
			filterCh <- *entry
		}
		// Skip the inputs we used.
		for i, r := range readers {
			if topEntries[i].name != "" {
				r.skip(1)
			}
		}
	}
	closeChannels()
	return filteredResults(), nil
}
