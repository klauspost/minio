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

type metacacheWriter struct {
	mw      *msgp.Writer
	creator func() error
	closer  func() error
}

func newMetacacheStream(out io.Writer) *metacacheWriter {
	w := metacacheWriter{
		mw: nil,
	}
	w.creator = func() error {
		s2w := s2EncPool.Get().(*s2.Writer)
		s2w.Reset(out)
		w.mw = msgp.NewWriter(s2w)
		w.creator = nil
		w.closer = func() error {
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
		w.closer = func() error {
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
func (w *metacacheWriter) write(objs ...metaCacheObject) error {
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
		err := w.mw.WriteString(o.name)
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

func (w *metacacheWriter) Close() error {
	if w == nil || w.closer == nil {
		return nil
	}
	err := w.closer()
	w.closer = nil
	return err
}

type metacacheReader struct {
	mr      *msgp.Reader
	current metaCacheObject
	closer  func()
}

func newMetacacheReader(r io.Reader) *metacacheReader {
	mr := msgp.NewReader(s2.NewReader(r))
	m := metacacheReader{
		mr: mr,
		closer: func() {
		},
	}
	return &m
}

// peek will return the name of the next object.
// Should be used sparingly.
func (w *metacacheReader) peek() (string, error) {
	if w.current.name != "" {
		return w.current.name, nil
	}
	var err error
	if w.current.name, err = w.mr.ReadString(); err != nil {
		return "", err
	}
	w.current.metadata, err = w.mr.ReadBytes(w.current.metadata[:0])
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return w.current.name, err
}

// next will read one entry from the stream.
// Generally not recommended for fast operation.
func (w *metacacheReader) next() (metaCacheObject, error) {
	var m metaCacheObject
	var err error
	if w.current.name != "" {
		m.name = w.current.name
		m.metadata = w.current.metadata
		w.current.name = ""
		w.current.metadata = nil
		return m, nil
	}
	if m.name, err = w.mr.ReadString(); err != nil {
		return m, err
	}
	m.metadata, err = w.mr.ReadBytes(nil)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}

	return m, err
}

// forwardTo will forward to the first entry that is >= s.
// Will return io.EOF if end of stream is reached without finding any.
func (w *metacacheReader) forwardTo(s string) error {
	if s == "" {
		return nil
	}
	// temporary name buffer.
	var tmp = make([]byte, 0, 256)
	for {
		// Read name without allocating more than 1 buffer.
		sz, err := w.mr.ReadStringHeader()
		if err != nil {
			return err
		}
		if cap(tmp) < int(sz) {
			tmp = make([]byte, 0, sz+256)
		}
		tmp = tmp[:sz]
		_, err = w.mr.R.ReadFull(tmp)
		if err != nil {
			return err
		}
		if string(tmp) >= s {
			w.current.name = string(tmp)
			w.current.metadata, err = w.mr.ReadBytes(nil)
			return err
		}
		// Skip metadata
		err = w.mr.Skip()
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
}

func (w *metacacheReader) readN(n int) (metaCacheObjectsSorted, error) {
	if n <= 0 {
		return metaCacheObjectsSorted{}, nil
	}
	res := make(metaCacheObjects, 0, n)
	if w.current.name != "" {
		res = append(res, w.current)
		w.current.name = ""
		w.current.metadata = nil
	}
	for len(res) < n {
		var err error
		var meta metaCacheObject
		if meta.name, err = w.mr.ReadString(); err != nil {
			return metaCacheObjectsSorted{o: res}, err
		}
		if meta.metadata, err = w.mr.ReadBytes(nil); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return metaCacheObjectsSorted{o: res}, err
		}
		res = append(res, meta)
	}
	return metaCacheObjectsSorted{o: res}, nil
}

// readAll will return all remaining objects on the dst channel and close it when done.
// The context allows the operation to be cancelled.
func (w *metacacheReader) readAll(ctx context.Context, dst chan<- metaCacheObject) error {
	defer close(dst)
	if w.current.name != "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case dst <- w.current:

		}
		w.current.name = ""
		w.current.metadata = nil
	}
	for {
		var err error
		var meta metaCacheObject
		if meta.name, err = w.mr.ReadString(); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if meta.metadata, err = w.mr.ReadBytes(nil); err != nil {
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
	return nil
}

// readNames will return all the requested number of names in order
// or all if n < 0.
// Will return io.EOF if end of stream is reached.
func (w *metacacheReader) readNames(n int) ([]string, error) {
	if n == 0 {
		return nil, nil
	}
	var res []string
	if n > 0 {
		res = make([]string, 0, n)
	}
	if w.current.name != "" {
		res = append(res, w.current.name)
		w.current.name = ""
		w.current.metadata = nil
	}
	for n < 0 || len(res) < n {
		var err error
		var name string
		if name, err = w.mr.ReadString(); err != nil {
			return nil, err
		}
		if err = w.mr.Skip(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return res, err
		}
		res = append(res, name)
	}
	return res, nil
}

func (w *metacacheReader) Close() error {
	if w == nil || w.closer == nil {
		return nil
	}
	w.closer()
	w.closer = nil
	return nil
}
