package cmd

import (
	"bytes"
	"context"
	"io"
	"path"
	"time"

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
	if o.Marker != "" {
		err = r.forwardTo(o.Marker)
		if err != nil {
			return entries, err
		}

		// Skip as long as marker matches exactly.
		for {
			obj, err := r.peek()
			if err != nil {
				return entries, err
			}
			if obj.name == o.Marker {
				err = r.skip(1)
				if err != nil {
					return entries, err
				}
			}
		}
	}
	// Filter
	if !o.Recursive {
		entries.o = make(metaCacheEntries, 0, o.Limit)
		// TODO: Check if we have to add a slash to the prefix sometimes?
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

	// We should not need to filter more.
	return r.readN(o.Limit, o.InclDeleted)
}

// Will return io.EOF if continuing would not yield more results.
func (er erasureObjects) listPath(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {
	// See if we have the listing stored.
	// Not really a loop, we break out if we are unable read and need to fall back.
	for {
		r, w := io.Pipe()
		err := er.getObject(ctx, minioMetaBucket, path.Join("buckets", o.Bucket, o.ID+".bin"), 0, -1, w, "", ObjectOptions{})
		if err != nil {
			break
		}
		mr, err := newMetacacheReader(r)
		if err != nil {
			break
		}
		defer mr.Close()
		return mr.filter(o)
	}

	// We need to ask disks.

	// Don't use disks that are healing
	healing, err := getAggregatedBackgroundHealState(ctx)
	if err != nil {
		logger.LogIf(ctx, err)
	}
	healDisks := make(map[string]struct{}, len(healing.HealDisks))
	for _, disk := range healing.HealDisks {
		healDisks[disk] = struct{}{}
	}

	var disks []StorageAPI
	for _, d := range er.getLoadBalancedDisks() {
		if d == nil || !d.IsOnline() {
			continue
		}
		di, err := d.DiskInfo(ctx)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		if _, ok := healDisks[di.Endpoint]; ok {
			continue
		}
		disks = append(disks, d)
	}

	const wantDisks = 3
	const askDisks = 4

	if len(disks) < wantDisks {
		err = InsufficientReadQuorum{}
		return
	}

	// Select askDisks random disks, 3 is ok.
	syncResults := make(chan io.ReadCloser, askDisks)
	if len(disks) < askDisks {
		for i := 0; i < askDisks-len(disks); i++ {
			syncResults <- nil
		}
	} else {
		disks = disks[:askDisks]
	}
	for i := range disks {
		go func(i int) {
			d := disks[i]
			// FIXME: nil writer
			err := d.WalkDir(ctx, WalkDirOptions{Bucket: o.Bucket, BaseDir: o.BaseDir, Recursive: o.Recursive || o.Separator != SlashSeparator}, nil)
			logger.LogIf(ctx, err)
			// FIXME no longer works
		}(i)
	}
	var readers []*metacacheReader
	var timeout <-chan time.Time
	got := 0

waitDrives:
	for {
		if got == askDisks {
			break waitDrives
		}
		select {
		case r, ok := <-syncResults:
			if !ok {
				break waitDrives
			}
			got++
			if r == nil {
				// A drive returned no results.
				continue
			}
			defer r.Close()
			mcr, err := newMetacacheReader(r)
			if err != nil {
				logger.LogIf(ctx, err)
				continue
			}
			defer mcr.Close()
			readers = append(readers, mcr)
			if len(readers) == wantDisks {
				// Wait additional 3 seconds for slow disk(s) to return.
				timer := time.NewTimer(3 * time.Second)
				defer timer.Stop()
				timeout = timer.C
			}
		case <-timeout:
			go func() {
				// Clean up ignored results.
				// TODO: We can cancel the requests being made on the remaining disks.
				for res := range syncResults {
					res.Close()
				}
			}()
			break waitDrives
		}
	}

	// FIXME: WRITE TO A CACHE AND FILTER RESULTS.
	// Merge results from the readers we got.
	if o.Limit > 0 {
		// Prealloc destination
		// FIXME: Should be dynamic filter.
		entries.o = make(metaCacheEntries, 0, o.Limit)
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
			entries.o = append(entries.o)
			continue
		}

		// Results Disagree :-(
		entry, ok := topEntries.resolve(&metadataResolutionParams{ /*TODO:*/ })
		if ok {
			entries.o = append(entries.o, *entry)
		}
		// Skip the inputs we used.
		for i, r := range readers {
			if topEntries[i].name != "" {
				r.skip(1)
			}
		}
	}

	return
}
