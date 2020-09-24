package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/tinylib/msgp/msgp"
)

var s2EncPool = sync.Pool{New: func() interface{} {
	return s2.NewWriter(nil)
}}

const metacacheStreamVersion = 1

// metacacheWriter provides a serializer of metacache objects.
type metacacheWriter struct {
	mw      *msgp.Writer
	creator func() error
	closer  func() error
}

// newMetacacheWriter will create a serializer that will write objects in given order to the output.
func newMetacacheWriter(out io.Writer) *metacacheWriter {
	w := metacacheWriter{
		mw: nil,
	}
	w.creator = func() error {
		s2w := s2EncPool.Get().(*s2.Writer)
		s2w.Reset(out)
		w.mw = msgp.NewWriter(s2w)
		w.creator = nil
		if err := w.mw.WriteByte(metacacheStreamVersion); err != nil {
			return err
		}

		w.closer = func() error {
			if err := w.mw.WriteBool(false); err != nil {
				return err
			}
			if err := w.mw.Flush(); err != nil {
				return err
			}
			err := s2w.Close()
			s2w.Reset(nil)
			s2EncPool.Put(s2w)
			return err
		}
		return nil
	}
	return &w
}

func newMetacacheFile(file string) *metacacheWriter {
	w := metacacheWriter{
		mw: nil,
	}
	w.creator = func() error {
		fw, err := os.Create(file)
		if err != nil {
			return err
		}
		s2w := s2EncPool.Get().(*s2.Writer)
		s2w.Reset(fw)
		w.mw = msgp.NewWriter(s2w)
		w.creator = nil
		if err := w.mw.WriteByte(metacacheStreamVersion); err != nil {
			return err
		}
		w.closer = func() error {
			// Indicate EOS
			if err := w.mw.WriteBool(false); err != nil {
				return err
			}
			if err := w.mw.Flush(); err != nil {
				fw.Close()
				return err
			}
			if err := s2w.Close(); err != nil {
				fw.Close()
				return err
			}
			s2w.Reset(nil)
			s2EncPool.Put(s2w)
			return fw.Close()
		}
		return nil
	}
	return &w
}

// write one or more objects to the stream in order.
// It is favorable to send as many objects as possible in a single write,
// but no more than math.MaxUint32
func (w *metacacheWriter) write(objs ...metaCacheEntry) error {
	if w == nil {
		return errors.New("metacacheWriter: nil writer")
	}
	if len(objs) == 0 {
		return nil
	}
	if w.creator != nil {
		err := w.creator()
		w.creator = nil
		if err != nil {
			return fmt.Errorf("metacacheWriter: unable to create writer: %w", err)
		}
		if w.mw == nil {
			return errors.New("metacacheWriter: writer not initialized")
		}
	}
	for _, o := range objs {
		if len(o.name) == 0 {
			return errors.New("metacacheWriter: no name provided")
		}
		// Indicate EOS
		err := w.mw.WriteBool(true)
		if err != nil {
			return err
		}
		err = w.mw.WriteString(o.name)
		if err != nil {
			return err
		}
		err = w.mw.WriteBytes(o.metadata)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close and release resources.
func (w *metacacheWriter) Close() error {
	if w == nil || w.closer == nil {
		return nil
	}
	err := w.closer()
	w.closer = nil
	return err
}

var s2DecPool = sync.Pool{New: func() interface{} {
	return s2.NewReader(nil)
}}

// metacacheReader allows reading a cache stream.
type metacacheReader struct {
	mr      *msgp.Reader
	current metaCacheEntry
	closer  func()
}

// newMetacacheReader creates a new cache reader.
// If no input or stream version is wrong an error is returned.
func newMetacacheReader(r io.Reader) (*metacacheReader, error) {
	dec := s2DecPool.Get().(*s2.Reader)
	dec.Reset(r)
	mr := msgp.NewReader(dec)
	m := metacacheReader{
		mr: mr,
		closer: func() {
			dec.Reset(nil)
			s2DecPool.Put(dec)
		},
	}
	v, err := mr.ReadByte()
	if err != nil {
		return nil, err
	}
	switch v {
	case metacacheStreamVersion:
	default:
		return nil, fmt.Errorf("metacacheReader: Unknown version: %d", v)
	}
	return &m, nil
}

// peek will return the name of the next object.
// Will return io.EOF if there are no more objects.
// Should be used sparingly.
func (r *metacacheReader) peek() (metaCacheEntry, error) {
	if r.current.name != "" {
		return r.current, nil
	}
	if more, err := r.mr.ReadBool(); !more {
		switch err {
		case nil:
			return metaCacheEntry{}, io.EOF
		case io.EOF:
			return metaCacheEntry{}, io.ErrUnexpectedEOF
		}
		return metaCacheEntry{}, err
	}

	var err error
	if r.current.name, err = r.mr.ReadString(); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return metaCacheEntry{}, err
	}
	r.current.metadata, err = r.mr.ReadBytes(r.current.metadata[:0])
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return r.current, err
}

// next will read one entry from the stream.
// Generally not recommended for fast operation.
func (r *metacacheReader) next() (metaCacheEntry, error) {
	var m metaCacheEntry
	var err error
	if r.current.name != "" {
		m.name = r.current.name
		m.metadata = r.current.metadata
		r.current.name = ""
		r.current.metadata = nil
		return m, nil
	}
	if more, err := r.mr.ReadBool(); !more {
		switch err {
		case nil:
			return m, io.EOF
		case io.EOF:
			return m, io.ErrUnexpectedEOF
		}
		return m, err
	}
	if m.name, err = r.mr.ReadString(); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return m, err
	}
	m.metadata, err = r.mr.ReadBytes(nil)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}

	return m, err
}

// forwardTo will forward to the first entry that is >= s.
// Will return io.EOF if end of stream is reached without finding any.
func (r *metacacheReader) forwardTo(s string) error {
	if s == "" {
		return nil
	}
	// temporary name buffer.
	var tmp = make([]byte, 0, 256)
	for {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case nil:
				return io.EOF
			case io.EOF:
				return io.ErrUnexpectedEOF
			}
			return err
		}
		// Read name without allocating more than 1 buffer.
		sz, err := r.mr.ReadStringHeader()
		if err != nil {
			return err
		}
		if cap(tmp) < int(sz) {
			tmp = make([]byte, 0, sz+256)
		}
		tmp = tmp[:sz]
		_, err = r.mr.R.ReadFull(tmp)
		if err != nil {
			return err
		}
		if string(tmp) >= s {
			r.current.name = string(tmp)
			r.current.metadata, err = r.mr.ReadBytes(nil)
			return err
		}
		// Skip metadata
		err = r.mr.Skip()
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
}

// readNames will return all the requested number of names in order
// or all if n < 0.
// Will return io.EOF if end of stream is reached.
// If requesting 0 objects nil error will always be returned regardless of at end of stream.
func (r *metacacheReader) readN(n int) (metaCacheEntriesSorted, error) {
	if n == 0 {
		return metaCacheEntriesSorted{}, nil
	}
	var res metaCacheEntries
	if n > 0 {
		res = make(metaCacheEntries, 0, n)
	}
	if r.current.name != "" {
		res = append(res, r.current)
		r.current.name = ""
		r.current.metadata = nil
	}
	for n < 0 || len(res) < n {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case nil:
				return metaCacheEntriesSorted{o: res}, io.EOF
			case io.EOF:
				return metaCacheEntriesSorted{o: res}, io.ErrUnexpectedEOF
			}
			return metaCacheEntriesSorted{o: res}, err
		}
		var err error
		var meta metaCacheEntry
		if meta.name, err = r.mr.ReadString(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return metaCacheEntriesSorted{o: res}, err
		}
		if meta.metadata, err = r.mr.ReadBytes(nil); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return metaCacheEntriesSorted{o: res}, err
		}
		res = append(res, meta)
	}
	return metaCacheEntriesSorted{o: res}, nil
}

// readAll will return all remaining objects on the dst channel and close it when done.
// The context allows the operation to be cancelled.
func (r *metacacheReader) readAll(ctx context.Context, dst chan<- metaCacheEntry) error {
	defer close(dst)
	if r.current.name != "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case dst <- r.current:

		}
		r.current.name = ""
		r.current.metadata = nil
	}
	for {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case io.EOF:
				return io.ErrUnexpectedEOF
			}
			return err
		}

		var err error
		var meta metaCacheEntry
		if meta.name, err = r.mr.ReadString(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		if meta.metadata, err = r.mr.ReadBytes(nil); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case dst <- meta:
		}
	}
}

// readFn will return all remaining objects
// and provide a callback for each entry read in order.
// The context allows the operation to be cancelled.
func (r *metacacheReader) readFn(ctx context.Context, fn func(entry metaCacheEntry)) error {
	if r.current.name != "" {
		fn(r.current)
		r.current.name = ""
		r.current.metadata = nil
	}
	for {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case io.EOF:
				return io.ErrUnexpectedEOF
			}
			return err
		}

		var err error
		var meta metaCacheEntry
		if meta.name, err = r.mr.ReadString(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		if meta.metadata, err = r.mr.ReadBytes(nil); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			fn(meta)
		}
	}
}

// readNames will return all the requested number of names in order
// or all if n < 0.
// Will return io.EOF if end of stream is reached.
func (r *metacacheReader) readNames(n int) ([]string, error) {
	if n == 0 {
		return nil, nil
	}
	var res []string
	if n > 0 {
		res = make([]string, 0, n)
	}
	if r.current.name != "" {
		res = append(res, r.current.name)
		r.current.name = ""
		r.current.metadata = nil
	}
	for n < 0 || len(res) < n {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case nil:
				return res, io.EOF
			case io.EOF:
				return res, io.ErrUnexpectedEOF
			}
			return res, err
		}

		var err error
		var name string
		if name, err = r.mr.ReadString(); err != nil {
			return res, err
		}
		if err = r.mr.Skip(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return res, err
		}
		res = append(res, name)
	}
	return res, nil
}

// skip n entries on the input stream.
// If there are less entries left io.EOF is returned.
func (r *metacacheReader) skip(n int) error {
	if n <= 0 {
		return nil
	}
	if r.current.name != "" {
		n--
		r.current.name = ""
		r.current.metadata = nil
	}
	for n > 0 {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case nil:
				return io.EOF
			case io.EOF:
				return io.ErrUnexpectedEOF
			}
			return err
		}

		if err := r.mr.Skip(); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if err := r.mr.Skip(); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		n--
	}
	return nil
}

// Close and release resources.
func (r *metacacheReader) Close() error {
	if r == nil || r.closer == nil {
		return nil
	}
	r.closer()
	r.closer = nil
	return nil
}
