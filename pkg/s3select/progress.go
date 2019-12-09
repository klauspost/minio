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

package s3select

import (
	"compress/bzip2"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
)

type countUpReader struct {
	reader    io.Reader
	bytesRead int64
}

func (r *countUpReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	atomic.AddInt64(&r.bytesRead, int64(n))
	return n, err
}

func (r *countUpReader) BytesRead() int64 {
	return atomic.LoadInt64(&r.bytesRead)
}

func newCountUpReader(reader io.Reader) *countUpReader {
	return &countUpReader{
		reader: reader,
	}
}

type progressReader struct {
	scannedReader   *countUpReader
	processedReader *countUpReader

	// Slice of closers to run
	close    []io.Closer
	closedMu sync.Mutex
	closed   bool
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	// This ensures that Close will block until Read has completed.
	// This allows another goroutine to close the reader.
	pr.closedMu.Lock()
	defer pr.closedMu.Unlock()
	if pr.closed {
		return 0, errors.New("progressReader: read after Close")
	}
	return pr.processedReader.Read(p)
}

func (pr *progressReader) Close() error {
	pr.closedMu.Lock()
	defer pr.closedMu.Unlock()
	if pr.closed {
		return nil
	}
	pr.closed = true
	var err error
	for _, c := range pr.close {
		err2 := c.Close()
		if err == nil && err2 != nil {
			err = err2
		}
	}
	return err
}

func (pr *progressReader) Stats() (bytesScanned, bytesProcessed int64) {
	return pr.scannedReader.BytesRead(), pr.processedReader.BytesRead()
}

func newProgressReader(rc io.ReadCloser, compType CompressionType) (*progressReader, error) {
	pr := progressReader{
		close: []io.Closer{rc},
	}

	scannedReader := newCountUpReader(rc)
	var r io.Reader
	var err error

	switch compType {
	case noneType:
		r = scannedReader
	case gzipType:
		rc, err = pgzip.NewReader(scannedReader)
		if err != nil {
			return nil, errTruncatedInput(err)
		}
		r = rc
		pr.close = append(pr.close, rc)
	case bzip2Type:
		r = bzip2.NewReader(scannedReader)
	case snappyType, s2Type:
		r = s2.NewReader(scannedReader)
	case zstdType:
		rc, err := zstd.NewReader(scannedReader)
		if err != nil {
			return nil, err
		}
		r = rc
		pr.close = append(pr.close, closeWrapper{fn: rc.Close})
	default:
		return nil, errInvalidCompressionFormat(fmt.Errorf("unknown compression type '%v'", compType))
	}
	pr.scannedReader = scannedReader
	pr.processedReader = newCountUpReader(r)

	return &pr, nil
}

// closeWrapper wraps a function call as a closer.
type closeWrapper struct {
	fn func()
}

func (c closeWrapper) Close() error {
	c.fn()
	return nil
}
