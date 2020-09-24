package cmd

import (
	"context"
	"io"
	"path"
)

type listPathOptions struct {
	// ID of the listing.
	// This will be used to persist the list.
	ID string
	// Bucket of the listing
	Bucket string

	// Scan/return only content with prefix.
	Prefix string

	// Marker to resume listing.
	// The response will be the first entry AFTER this object name.
	Marker string

	// Limit the number of results.
	Limit int

	// Scan recursively.
	// If false only
	Recursive bool

	// Separator to use.
	Separator string
}

type listPathResponse struct {
	ID      string
	Entries metaCacheEntriesSorted
	Done    bool
}

// filter will apply the options and return the
func (o listPathOptions) filter(r *metacacheReader) (entries metaCacheEntriesSorted, err error) {
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
	return
}

// Will return io.EOF if continuing would not yield more results.
func (er erasureObjects) listPath(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {

	// See if we have the listing stored.
	// Not really a loop.
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
		return entries, err
	}
	return
}
