package cmd

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"reflect"
	"testing"
)

var loadMetacacheSampleNames = []string{"src/compress/bzip2/", "src/compress/bzip2/bit_reader.go", "src/compress/bzip2/bzip2.go", "src/compress/bzip2/bzip2_test.go", "src/compress/bzip2/huffman.go", "src/compress/bzip2/move_to_front.go", "src/compress/bzip2/testdata/", "src/compress/bzip2/testdata/Isaac.Newton-Opticks.txt.bz2", "src/compress/bzip2/testdata/e.txt.bz2", "src/compress/bzip2/testdata/fail-issue5747.bz2", "src/compress/bzip2/testdata/pass-random1.bin", "src/compress/bzip2/testdata/pass-random1.bz2", "src/compress/bzip2/testdata/pass-random2.bin", "src/compress/bzip2/testdata/pass-random2.bz2", "src/compress/bzip2/testdata/pass-sawtooth.bz2", "src/compress/bzip2/testdata/random.data.bz2", "src/compress/flate/", "src/compress/flate/deflate.go", "src/compress/flate/deflate_test.go", "src/compress/flate/deflatefast.go", "src/compress/flate/dict_decoder.go", "src/compress/flate/dict_decoder_test.go", "src/compress/flate/example_test.go", "src/compress/flate/flate_test.go", "src/compress/flate/huffman_bit_writer.go", "src/compress/flate/huffman_bit_writer_test.go", "src/compress/flate/huffman_code.go", "src/compress/flate/inflate.go", "src/compress/flate/inflate_test.go", "src/compress/flate/reader_test.go", "src/compress/flate/testdata/", "src/compress/flate/testdata/huffman-null-max.dyn.expect", "src/compress/flate/testdata/huffman-null-max.dyn.expect-noinput", "src/compress/flate/testdata/huffman-null-max.golden", "src/compress/flate/testdata/huffman-null-max.in", "src/compress/flate/testdata/huffman-null-max.wb.expect", "src/compress/flate/testdata/huffman-null-max.wb.expect-noinput", "src/compress/flate/testdata/huffman-pi.dyn.expect", "src/compress/flate/testdata/huffman-pi.dyn.expect-noinput", "src/compress/flate/testdata/huffman-pi.golden", "src/compress/flate/testdata/huffman-pi.in", "src/compress/flate/testdata/huffman-pi.wb.expect", "src/compress/flate/testdata/huffman-pi.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.golden", "src/compress/flate/testdata/huffman-rand-1k.in", "src/compress/flate/testdata/huffman-rand-1k.wb.expect", "src/compress/flate/testdata/huffman-rand-1k.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.golden", "src/compress/flate/testdata/huffman-rand-limit.in", "src/compress/flate/testdata/huffman-rand-limit.wb.expect", "src/compress/flate/testdata/huffman-rand-limit.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-max.golden", "src/compress/flate/testdata/huffman-rand-max.in", "src/compress/flate/testdata/huffman-shifts.dyn.expect", "src/compress/flate/testdata/huffman-shifts.dyn.expect-noinput", "src/compress/flate/testdata/huffman-shifts.golden", "src/compress/flate/testdata/huffman-shifts.in", "src/compress/flate/testdata/huffman-shifts.wb.expect", "src/compress/flate/testdata/huffman-shifts.wb.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.dyn.expect", "src/compress/flate/testdata/huffman-text-shift.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.golden", "src/compress/flate/testdata/huffman-text-shift.in", "src/compress/flate/testdata/huffman-text-shift.wb.expect", "src/compress/flate/testdata/huffman-text-shift.wb.expect-noinput", "src/compress/flate/testdata/huffman-text.dyn.expect", "src/compress/flate/testdata/huffman-text.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text.golden", "src/compress/flate/testdata/huffman-text.in", "src/compress/flate/testdata/huffman-text.wb.expect", "src/compress/flate/testdata/huffman-text.wb.expect-noinput", "src/compress/flate/testdata/huffman-zero.dyn.expect", "src/compress/flate/testdata/huffman-zero.dyn.expect-noinput", "src/compress/flate/testdata/huffman-zero.golden", "src/compress/flate/testdata/huffman-zero.in", "src/compress/flate/testdata/huffman-zero.wb.expect", "src/compress/flate/testdata/huffman-zero.wb.expect-noinput", "src/compress/flate/testdata/null-long-match.dyn.expect-noinput", "src/compress/flate/testdata/null-long-match.wb.expect-noinput", "src/compress/flate/token.go", "src/compress/flate/writer_test.go", "src/compress/gzip/", "src/compress/gzip/example_test.go", "src/compress/gzip/gunzip.go", "src/compress/gzip/gunzip_test.go", "src/compress/gzip/gzip.go", "src/compress/gzip/gzip_test.go", "src/compress/gzip/issue14937_test.go", "src/compress/gzip/testdata/", "src/compress/gzip/testdata/issue6550.gz.base64", "src/compress/lzw/", "src/compress/lzw/reader.go", "src/compress/lzw/reader_test.go", "src/compress/lzw/writer.go", "src/compress/lzw/writer_test.go", "src/compress/testdata/", "src/compress/testdata/e.txt", "src/compress/testdata/gettysburg.txt", "src/compress/testdata/pi.txt", "src/compress/zlib/", "src/compress/zlib/example_test.go", "src/compress/zlib/reader.go", "src/compress/zlib/reader_test.go", "src/compress/zlib/writer.go", "src/compress/zlib/writer_test.go"}

func loadMetacacheSample(t testing.TB) *metacacheReader {
	b, err := ioutil.ReadFile("testdata/metacache.s2")
	if err != nil {
		t.Fatal(err)
	}
	return newMetacacheReader(bytes.NewBuffer(b))
}

func loadMetacacheSampleEntries(t testing.TB) metaCacheEntriesSorted {
	r := loadMetacacheSample(t)
	defer r.Close()
	entries, err := r.readN(-1)
	if err != io.EOF {
		t.Fatal(err)
	}
	return entries
}

func Test_metacacheReader_readNames(t *testing.T) {
	r := loadMetacacheSample(t)
	defer r.Close()
	names, err := r.readNames(-1)
	if err != io.EOF {
		t.Fatal(err)
	}
	want := loadMetacacheSampleNames
	if !reflect.DeepEqual(names, want) {
		t.Errorf("got unexpected result: %#v", names)
	}
}

func Test_metacacheReader_readN(t *testing.T) {
	r := loadMetacacheSample(t)
	defer r.Close()
	entries, err := r.readN(-1)
	if err != io.EOF {
		t.Fatal(err, entries.len())
	}
	want := loadMetacacheSampleNames
	for i, entry := range entries.entries() {
		if entry.name != want[i] {
			t.Errorf("entry %d, want %q, got %q", i, want[i], entry.name)
		}
	}
	if entries.len() != len(want) {
		t.Fatal("unexpected length:", entries.len(), "want:", len(want))
	}

	want = want[:0]
	entries, err = r.readN(0)
	if err != nil {
		t.Fatal(err, entries.len())
	}
	if entries.len() != len(want) {
		t.Fatal("unexpected length:", entries.len(), "want:", len(want))
	}
	r = loadMetacacheSample(t)
	defer r.Close()
	entries, err = r.readN(0)
	if err != nil {
		t.Fatal(err, entries.len())
	}
	if entries.len() != len(want) {
		t.Fatal("unexpected length:", entries.len(), "want:", len(want))
	}

	entries, err = r.readN(5)
	if err != nil {
		t.Fatal(err, entries.len())
	}
	want = loadMetacacheSampleNames[:5]
	if entries.len() != len(want) {
		t.Fatal("unexpected length:", entries.len(), "want:", len(want))
	}

	for i, entry := range entries.entries() {
		if entry.name != want[i] {
			t.Errorf("entry %d, want %q, got %q", i, want[i], entry.name)
		}
	}
}

func Test_metacacheReader_readFn(t *testing.T) {
	r := loadMetacacheSample(t)
	defer r.Close()
	i := 0
	err := r.readFn(context.Background(), func(entry metaCacheEntry) {
		want := loadMetacacheSampleNames[i]
		if entry.name != want {
			t.Errorf("entry %d, want %q, got %q", i, want, entry.name)
		}
		i++
	})
	if err != nil {
		t.Fatal(err)
	}
}

func Test_metacacheReader_readAll(t *testing.T) {
	r := loadMetacacheSample(t)
	defer r.Close()
	var readErr error
	objs := make(chan metaCacheEntry, 1)
	go func() {
		readErr = r.readAll(context.Background(), objs)
	}()
	want := loadMetacacheSampleNames
	i := 0
	for entry := range objs {
		if entry.name != want[i] {
			t.Errorf("entry %d, want %q, got %q", i, want[i], entry.name)
		}
		i++
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
}

func Test_metacacheReader_forwardTo(t *testing.T) {
	r := loadMetacacheSample(t)
	defer r.Close()
	err := r.forwardTo("src/compress/zlib/reader_test.go")
	if err != nil {
		t.Fatal(err)
	}
	names, err := r.readNames(-1)
	if err != io.EOF {
		t.Fatal(err)
	}
	want := []string{"src/compress/zlib/reader_test.go", "src/compress/zlib/writer.go", "src/compress/zlib/writer_test.go"}
	if !reflect.DeepEqual(names, want) {
		t.Errorf("got unexpected result: %#v", names)
	}

	// Try with prefix
	r = loadMetacacheSample(t)
	err = r.forwardTo("src/compress/zlib/reader_t")
	if err != nil {
		t.Fatal(err)
	}
	names, err = r.readNames(-1)
	if err != io.EOF {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, want) {
		t.Errorf("got unexpected result: %#v", names)
	}
}

func Test_metacacheReader_next(t *testing.T) {
	r := loadMetacacheSample(t)
	defer r.Close()
	for i, want := range loadMetacacheSampleNames {
		gotObj, err := r.next()
		if err != nil {
			t.Fatal(err)
		}
		if gotObj.name != want {
			t.Errorf("entry %d, want %q, got %q", i, want, gotObj.name)
		}
	}
}
func Test_metacacheReader_peek(t *testing.T) {
	r := loadMetacacheSample(t)
	defer r.Close()
	for i, want := range loadMetacacheSampleNames {
		got, err := r.peek()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("entry %d, want %q, got %q", i, want, got)
		}
		gotObj, err := r.next()
		if err != nil {
			t.Fatal(err)
		}
		if gotObj.name != want {
			t.Errorf("entry %d, want %q, got %q", i, want, gotObj.name)
		}
	}
}

func Test_newMetacacheStream(t *testing.T) {
	r := loadMetacacheSample(t)
	var buf bytes.Buffer
	w := newMetacacheWriter(&buf)
	err := r.readFn(context.Background(), func(object metaCacheEntry) {
		err := w.write(object)
		if err != nil {
			t.Fatal(err)
		}
	})
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	r = newMetacacheReader(&buf)
	defer r.Close()
	names, err := r.readNames(-1)
	if err != io.EOF {
		t.Fatal(err)
	}
	want := loadMetacacheSampleNames
	if !reflect.DeepEqual(names, want) {
		t.Errorf("got unexpected result: %#v", names)
	}
}

func Test_metacacheReader_skip(t *testing.T) {
	r := loadMetacacheSample(t)
	defer r.Close()
	names, err := r.readNames(5)
	if err != nil {
		t.Fatal(err)
	}
	want := loadMetacacheSampleNames[:5]
	if !reflect.DeepEqual(names, want) {
		t.Errorf("got unexpected result: %#v", names)
	}
	err = r.skip(5)
	if err != nil {
		t.Fatal(err)
	}
	names, err = r.readNames(5)
	if err != nil {
		t.Fatal(err)
	}
	want = loadMetacacheSampleNames[10:15]
	if !reflect.DeepEqual(names, want) {
		t.Errorf("got unexpected result: %#v", names)
	}

	err = r.skip(len(loadMetacacheSampleNames))
	if err != io.EOF {
		t.Fatal(err)
	}
}
