package cmd

import (
	"context"
	"io"
	"path"
)

type listPathOptions struct {
	ID        string
	Bucket    string
	Marker    string
	Prefix    string
	Limit     int
	Recursive bool
}

type listPathResponse struct {
	ID      string
	Entries metaCacheEntriesSorted
	Done    bool
}

// Will return io.EOF if continuing would not yield more results.
func (er erasureObjects) listPath(ctx context.Context, o listPathOptions) (entries metaCacheEntriesSorted, err error) {

	if o.Marker == "" {
		o.Marker = o.Prefix
	}
	// See if we have the listing stored.
	{
		r, w := io.Pipe()
		err := er.getObject(ctx, minioMetaBucket, path.Join("buckets", o.Bucket, o.ID+".bin"), 0, -1, w, "", ObjectOptions{})
		if err == nil {
			r := newMetacacheReader(r)
			err := r.forwardTo(o.Marker)
			if err != nil {
				return entries, err
			}
		}
	}
}
