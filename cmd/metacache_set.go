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
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
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

	// CurrentCycle indicates the current bloom cycle.
	// Will be used if a new scan is started.
	CurrentCycle uint64

	// OldestCycle indicates the oldest cycle acceptable.
	OldestCycle uint64
}

// gatherResults will collect all results on the input channel and filter results according to the options.
// Caller should close the channel when done.
// The returned function will return the results once there is enough or input is closed.
func (o *listPathOptions) gatherResults(in <-chan metaCacheEntry) func() metaCacheEntriesSorted {
	var resultsDone = make(chan metaCacheEntriesSorted)
	// Copy so we can mutate
	resCh := resultsDone
	go func() {
		var results metaCacheEntriesSorted
		for entry := range in {
			if o.Limit > 0 && results.len() > o.Limit {
				if resCh != nil {
					resCh <- results
					resCh = nil
				}
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
		if resCh != nil {
			resCh <- results
			resCh = nil
		}
	}()
	return func() metaCacheEntriesSorted {
		return <-resultsDone
	}
}

// findFirstPart will find the part with 0 being the first that corresponds to the marker in the options.
// io.ErrUnexpectedEOF is returned if the place containing the marker hasn't been scanned yet.
// io.EOF indicates the marker is beyond the end of the stream and does not exist.
func (o *listPathOptions) findFirstPart(fi FileInfo) (int, error) {
	search := o.Marker
	if search == "" {
		search = o.Prefix
	}
	if search == "" {
		return 0, nil
	}
	fmt.Println("searching for ", search)
	var tmp metacacheBlock
	i := 0
	for {
		partKey := fmt.Sprintf("%s-metacache-part-%d", ReservedMetadataPrefixLower, i)
		v, ok := fi.Metadata[partKey]
		if !ok {
			fmt.Println("no match in metadata, waiting")
			return -1, io.ErrUnexpectedEOF
		}
		err := json.Unmarshal([]byte(v), &tmp)
		if !ok {
			logger.LogIf(context.Background(), err)
			return -1, err
		}
		if tmp.First >= search {
			fmt.Println("First >= search", v)
			return i, nil
		}
		if tmp.Last >= search {
			fmt.Println("Last >= search", v)
			return i, nil
		}
		if tmp.EOS {
			fmt.Println("no match, at EOS", v)
			return -3, io.EOF
		}
		//fmt.Println("First ", tmp.First, "<", search, " search", i)
		i++
	}
	return -2, io.ErrUnexpectedEOF
}

func getMetacacheBlockInfo(fi FileInfo, block int) (*metacacheBlock, error) {
	var tmp metacacheBlock
	partKey := fmt.Sprintf("%s-metacache-part-%d", ReservedMetadataPrefixLower, block)
	v, ok := fi.Metadata[partKey]
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	return &tmp, json.Unmarshal([]byte(v), &tmp)
}

func metacachePrefixForID(bucket, id string) string {
	return pathJoin("buckets", bucket, ".metacache", id)
}

// objectPath returns the object path of the cache.
func (o *listPathOptions) objectPath(block int) string {
	return pathJoin(metacachePrefixForID(o.Bucket, o.ID), "block-", strconv.Itoa(block)+".s2")
}

// filter will apply the options and return the number of objects requested by the limit.
// Will return io.EOF if there are no more entries with the same filter.
// The last entry can be used as a marker to resume the listing.
func (r *metacacheReader) filter(o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	// Forward to prefix, if any
	err = r.forwardTo(o.Prefix)
	if err != nil {
		return entries, err
	}
	if o.Marker != "" {
		err = r.forwardTo(o.Marker)
		if err != nil {
			return entries, err
		}
	}
	fmt.Println("forwarded to ", o.Prefix, "marker:", o.Marker)
	// Filter
	if !o.Recursive {
		entries.o = make(metaCacheEntries, 0, o.Limit)
		pastPrefix := false
		err := r.readFn(func(entry metaCacheEntry) bool {
			if o.Prefix != "" && !strings.HasPrefix(entry.name, o.Prefix) {
				// We are past the prefix, don't continue.
				pastPrefix = true
				return false
			}
			if o.InclDeleted && entry.isObject() && entry.isLatestDeletemarker() {
				return entries.len() >= o.Limit
			}
			if entry.isInDir(o.Prefix, o.Separator) {
				entries.o = append(entries.o, entry)
			}
			return entries.len() >= o.Limit
		})
		if err == io.EOF || pastPrefix {
			return entries, io.EOF
		}
		return entries, err
	}

	fmt.Println("returning", o.Limit, "results")
	defer fmt.Println("returned")

	// We should not need to filter more.
	//  FIXME: Really, what about stopping when we get outside the requested prefix?
	return r.readN(o.Limit, o.InclDeleted, o.Prefix)
}

func (er erasureObjects) streamMetadataParts(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	partN := -1
	for {
		select {
		case <-ctx.Done():
			return entries, ctx.Err()
		default:
		}
		// Load first part metadata...
		fi, metaArr, onlineDisks, err := er.getObjectFileInfo(ctx, minioMetaBucket, o.objectPath(0), ObjectOptions{})
		if err != nil {
			if err == errFileNotFound {
				// Not ready yet...
				time.Sleep(100 * time.Millisecond)
				continue
			}

			fmt.Println("first getObjectFileInfo", o.objectPath(0), "returned err:", err)
			return entries, err
		}
		if fi.Deleted {
			fmt.Println(o.objectPath(0), "deleted")
			return entries, errFileNotFound
		}

		partN, err = o.findFirstPart(fi)
		switch err {
		case nil:
		case io.ErrUnexpectedEOF:
			time.Sleep(100 * time.Millisecond)
			continue
		case io.EOF:
			fmt.Println("findFirstPart EOF", fi.Metadata)
			return entries, io.EOF
		}
		// We got a stream to start at.
		loadedPart := 0
		var buf bytes.Buffer
		for {
			if partN != loadedPart {
				// Load first part metadata...
				fi, metaArr, onlineDisks, err = er.getObjectFileInfo(ctx, minioMetaBucket, o.objectPath(partN), ObjectOptions{})
				switch err {
				case errFileNotFound:
					// Not ready yet...
					time.Sleep(100 * time.Millisecond)
					continue
				default:
					fmt.Println("getObjectFileInfo err", o.objectPath(partN), fi.Metadata, err)
					return entries, err
				case nil:
					loadedPart = partN
				}
				if fi.Deleted {
					return entries, io.ErrUnexpectedEOF
				}
			}
			buf.Reset()
			err := er.getObjectWithFileInfo(ctx, minioMetaBucket, o.objectPath(partN), 0, fi.Size, &buf, fi, metaArr, onlineDisks)
			if err != nil {
				fmt.Println("getObjectWithFileInfo err", o.objectPath(partN), fi.Metadata, err)
				return entries, err
			}
			tmp, err := newMetacacheReader(&buf)
			if err != nil {
				return entries, err
			}
			e, err := tmp.filter(o)
			entries.o = append(entries.o, e.o...)
			if o.Limit > 0 && entries.len() > o.Limit {
				entries.truncate(o.Limit)
				return entries, nil
			}
			switch err {
			case io.EOF:
				// We finished at the end of the block.
				// And should not expect any more results.
				bi, err := getMetacacheBlockInfo(fi, partN)
				logger.LogIf(ctx, err)
				if err != nil || bi.EOS {
					// We are done and there are no more parts.
					return entries, io.EOF
				}
				partN++
			case nil:
				// We stopped within the listing, we are done for now...
				return entries, nil
			default:
				return entries, err
			}
		}
	}
}

// Will return io.EOF if continuing would not yield more results.
func (er erasureObjects) listPath(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	startTime := time.Now()
	fmt.Println("set listing bucket:", o.Bucket, "basedir:", o.BaseDir)
	// See if we have the listing stored.
	if !o.Create {
		entries, err := er.streamMetadataParts(ctx, o)
		switch err {
		case nil, io.EOF, context.Canceled, context.DeadlineExceeded:
			return entries, err
		}
		logger.LogIf(ctx, err)
		// TODO: Should we start listing???
		return entries, err
	}

	// Disconnect from call above.
	ctx = context.Background()
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

	// FIXME: Return when we have enough results...
	// Create output for our results.
	const blockSize = 5000
	cacheCh := make(chan metaCacheEntry, blockSize)

	// Create filter for results.
	filterCh := make(chan metaCacheEntry, 100)
	filteredResults := o.gatherResults(filterCh)
	closeChannels := func() {
		close(cacheCh)
		close(filterCh)
	}

	go func() {
		// Write results to disk.
		bw := newMetacacheBlockWriter(cacheCh, func(b *metacacheBlock) error {
			fmt.Println("SAVING BLOCK", b.n, "to", o.objectPath(b.n))
			r, err := hash.NewReader(bytes.NewBuffer(b.data), int64(len(b.data)), "", "", int64(len(b.data)), false)
			logger.LogIf(ctx, err)
			meta := b.headerKV()
			meta[xhttp.AmzStorageClass] = storageclass.RRS
			_, err = er.putObject(ctx, minioMetaBucket, o.objectPath(b.n), NewPutObjReader(r, nil, nil), ObjectOptions{UserDefined: meta})
			if err != nil || b.n == 0 {
				logger.LogIf(ctx, err)
				return err
			}
			// Update block 0 metadata.
			// TODO: Potentially a bit risky. Should we just retry?
			return er.updateObjectMeta(ctx, minioMetaBucket, o.objectPath(0), b.headerKV(), ObjectOptions{})
		})
		defer bw.Close()

		// Merge results from the readers.
		resolver := metadataResolutionParams{
			startTime: startTime,
			dirQuorum: askDisks - 1,
			bucket:    o.Bucket,
		}

		topEntries := make(metaCacheEntries, len(readers))
		for {
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
					// FIXME: Handle error
					return
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
	}()

	return filteredResults(), nil
}
