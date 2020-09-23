package cmd

import (
	"bytes"
	"reflect"
	"testing"
)

func Test_metaCacheEntries_sort(t *testing.T) {
	entries := loadMetacacheSampleEntries(t)

	o := entries.entries()
	if !o.isSorted() {
		t.Fatal("Expected sorted objects")
	}

	// Swap first and last
	o[0], o[len(o)-1] = o[len(o)-1], o[0]
	if o.isSorted() {
		t.Fatal("Expected unsorted objects")
	}

	sorted := o.sort()
	if !o.isSorted() {
		t.Fatal("Expected sorted o objects")
	}
	if !sorted.entries().isSorted() {
		t.Fatal("Expected sorted wrapped objects")
	}
	want := loadMetacacheSampleNames
	for i, got := range o {
		if got.name != want[i] {
			t.Errorf("entry %d, want %q, got %q", i, want[i], got.name)
		}
	}
}

func Test_metaCacheEntries_forwardTo(t *testing.T) {
	org := loadMetacacheSampleEntries(t)
	entries := org
	want := []string{"src/compress/zlib/reader_test.go", "src/compress/zlib/writer.go", "src/compress/zlib/writer_test.go"}
	entries.forwardTo("src/compress/zlib/reader_test.go")
	got := entries.entries().names()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got unexpected result: %#v", got)
	}

	// Try with prefix
	entries = org
	entries.forwardTo("src/compress/zlib/reader_t")
	got = entries.entries().names()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_merge(t *testing.T) {
	org := loadMetacacheSampleEntries(t)
	a, b := org.shallowClone(), org.shallowClone()

	// Merge b into a
	a.merge(b)
	want := loadMetacacheSampleNames
	got := a.entries().names()
	if len(got) != len(want)*2 {
		t.Errorf("unexpected count, want %v, got %v", len(want)*2, len(got))
	}

	for i, name := range got {
		if want[i/2] != name {
			t.Errorf("unexpected name, want %q, got %q", want[i/2], name)
		}
	}
}

func Test_metaCacheEntries_dedupe(t *testing.T) {
	org := loadMetacacheSampleEntries(t)
	a, b := org.shallowClone(), org.shallowClone()

	// Merge b into a
	a.merge(b)
	if a.deduplicate(nil) {
		t.Fatal("deduplicate returned duplicate entries left")
	}
	want := loadMetacacheSampleNames
	got := a.entries().names()
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_dedupe2(t *testing.T) {
	org := loadMetacacheSampleEntries(t)
	a, b := org.shallowClone(), org.shallowClone()

	// Replace metadata in b
	testMarker := []byte("sampleset")
	for i := range b.o {
		b.o[i].metadata = testMarker
	}

	// Merge b into a
	a.merge(b)
	if a.deduplicate(func(existing, other []byte) (replace bool) {
		a := bytes.Equal(existing, testMarker)
		b := bytes.Equal(other, testMarker)
		if a == b {
			t.Fatal("got same number of testmarkers, only one should be given", a, b)
		}
		return b
	}) {
		t.Fatal("deduplicate returned duplicate entries left, we should always resolve")
	}
	want := loadMetacacheSampleNames
	got := a.entries().names()
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterObjects(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterObjectsOnly()
	got := data.entries().names()
	want := []string{"src/compress/bzip2/bit_reader.go", "src/compress/bzip2/bzip2.go", "src/compress/bzip2/bzip2_test.go", "src/compress/bzip2/huffman.go", "src/compress/bzip2/move_to_front.go", "src/compress/bzip2/testdata/Isaac.Newton-Opticks.txt.bz2", "src/compress/bzip2/testdata/e.txt.bz2", "src/compress/bzip2/testdata/fail-issue5747.bz2", "src/compress/bzip2/testdata/pass-random1.bin", "src/compress/bzip2/testdata/pass-random1.bz2", "src/compress/bzip2/testdata/pass-random2.bin", "src/compress/bzip2/testdata/pass-random2.bz2", "src/compress/bzip2/testdata/pass-sawtooth.bz2", "src/compress/bzip2/testdata/random.data.bz2", "src/compress/flate/deflate.go", "src/compress/flate/deflate_test.go", "src/compress/flate/deflatefast.go", "src/compress/flate/dict_decoder.go", "src/compress/flate/dict_decoder_test.go", "src/compress/flate/example_test.go", "src/compress/flate/flate_test.go", "src/compress/flate/huffman_bit_writer.go", "src/compress/flate/huffman_bit_writer_test.go", "src/compress/flate/huffman_code.go", "src/compress/flate/inflate.go", "src/compress/flate/inflate_test.go", "src/compress/flate/reader_test.go", "src/compress/flate/testdata/huffman-null-max.dyn.expect", "src/compress/flate/testdata/huffman-null-max.dyn.expect-noinput", "src/compress/flate/testdata/huffman-null-max.golden", "src/compress/flate/testdata/huffman-null-max.in", "src/compress/flate/testdata/huffman-null-max.wb.expect", "src/compress/flate/testdata/huffman-null-max.wb.expect-noinput", "src/compress/flate/testdata/huffman-pi.dyn.expect", "src/compress/flate/testdata/huffman-pi.dyn.expect-noinput", "src/compress/flate/testdata/huffman-pi.golden", "src/compress/flate/testdata/huffman-pi.in", "src/compress/flate/testdata/huffman-pi.wb.expect", "src/compress/flate/testdata/huffman-pi.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.golden", "src/compress/flate/testdata/huffman-rand-1k.in", "src/compress/flate/testdata/huffman-rand-1k.wb.expect", "src/compress/flate/testdata/huffman-rand-1k.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.golden", "src/compress/flate/testdata/huffman-rand-limit.in", "src/compress/flate/testdata/huffman-rand-limit.wb.expect", "src/compress/flate/testdata/huffman-rand-limit.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-max.golden", "src/compress/flate/testdata/huffman-rand-max.in", "src/compress/flate/testdata/huffman-shifts.dyn.expect", "src/compress/flate/testdata/huffman-shifts.dyn.expect-noinput", "src/compress/flate/testdata/huffman-shifts.golden", "src/compress/flate/testdata/huffman-shifts.in", "src/compress/flate/testdata/huffman-shifts.wb.expect", "src/compress/flate/testdata/huffman-shifts.wb.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.dyn.expect", "src/compress/flate/testdata/huffman-text-shift.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.golden", "src/compress/flate/testdata/huffman-text-shift.in", "src/compress/flate/testdata/huffman-text-shift.wb.expect", "src/compress/flate/testdata/huffman-text-shift.wb.expect-noinput", "src/compress/flate/testdata/huffman-text.dyn.expect", "src/compress/flate/testdata/huffman-text.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text.golden", "src/compress/flate/testdata/huffman-text.in", "src/compress/flate/testdata/huffman-text.wb.expect", "src/compress/flate/testdata/huffman-text.wb.expect-noinput", "src/compress/flate/testdata/huffman-zero.dyn.expect", "src/compress/flate/testdata/huffman-zero.dyn.expect-noinput", "src/compress/flate/testdata/huffman-zero.golden", "src/compress/flate/testdata/huffman-zero.in", "src/compress/flate/testdata/huffman-zero.wb.expect", "src/compress/flate/testdata/huffman-zero.wb.expect-noinput", "src/compress/flate/testdata/null-long-match.dyn.expect-noinput", "src/compress/flate/testdata/null-long-match.wb.expect-noinput", "src/compress/flate/token.go", "src/compress/flate/writer_test.go", "src/compress/gzip/example_test.go", "src/compress/gzip/gunzip.go", "src/compress/gzip/gunzip_test.go", "src/compress/gzip/gzip.go", "src/compress/gzip/gzip_test.go", "src/compress/gzip/issue14937_test.go", "src/compress/gzip/testdata/issue6550.gz.base64", "src/compress/lzw/reader.go", "src/compress/lzw/reader_test.go", "src/compress/lzw/writer.go", "src/compress/lzw/writer_test.go", "src/compress/testdata/e.txt", "src/compress/testdata/gettysburg.txt", "src/compress/testdata/pi.txt", "src/compress/zlib/example_test.go", "src/compress/zlib/reader.go", "src/compress/zlib/reader_test.go", "src/compress/zlib/writer.go", "src/compress/zlib/writer_test.go"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterPrefixes(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterPrefixesOnly()
	got := data.entries().names()
	want := []string{"src/compress/bzip2/", "src/compress/bzip2/testdata/", "src/compress/flate/", "src/compress/flate/testdata/", "src/compress/gzip/", "src/compress/gzip/testdata/", "src/compress/lzw/", "src/compress/testdata/", "src/compress/zlib/"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterRecursive(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterRecursiveEntries("src/compress/bzip2/", slashSeparator)
	got := data.entries().names()
	want := []string{"src/compress/bzip2/", "src/compress/bzip2/bit_reader.go", "src/compress/bzip2/bzip2.go", "src/compress/bzip2/bzip2_test.go", "src/compress/bzip2/huffman.go", "src/compress/bzip2/move_to_front.go"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterRecursiveRoot(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterRecursiveEntries("", slashSeparator)
	got := data.entries().names()
	want := []string{}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterRecursiveRootSep(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	// This will remove anything with "bzip2/" in the path since it is separator
	data.filterRecursiveEntries("", "bzip2/")
	got := data.entries().names()
	want := []string{"src/compress/flate/", "src/compress/flate/deflate.go", "src/compress/flate/deflate_test.go", "src/compress/flate/deflatefast.go", "src/compress/flate/dict_decoder.go", "src/compress/flate/dict_decoder_test.go", "src/compress/flate/example_test.go", "src/compress/flate/flate_test.go", "src/compress/flate/huffman_bit_writer.go", "src/compress/flate/huffman_bit_writer_test.go", "src/compress/flate/huffman_code.go", "src/compress/flate/inflate.go", "src/compress/flate/inflate_test.go", "src/compress/flate/reader_test.go", "src/compress/flate/testdata/", "src/compress/flate/testdata/huffman-null-max.dyn.expect", "src/compress/flate/testdata/huffman-null-max.dyn.expect-noinput", "src/compress/flate/testdata/huffman-null-max.golden", "src/compress/flate/testdata/huffman-null-max.in", "src/compress/flate/testdata/huffman-null-max.wb.expect", "src/compress/flate/testdata/huffman-null-max.wb.expect-noinput", "src/compress/flate/testdata/huffman-pi.dyn.expect", "src/compress/flate/testdata/huffman-pi.dyn.expect-noinput", "src/compress/flate/testdata/huffman-pi.golden", "src/compress/flate/testdata/huffman-pi.in", "src/compress/flate/testdata/huffman-pi.wb.expect", "src/compress/flate/testdata/huffman-pi.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect", "src/compress/flate/testdata/huffman-rand-1k.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-1k.golden", "src/compress/flate/testdata/huffman-rand-1k.in", "src/compress/flate/testdata/huffman-rand-1k.wb.expect", "src/compress/flate/testdata/huffman-rand-1k.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect", "src/compress/flate/testdata/huffman-rand-limit.dyn.expect-noinput", "src/compress/flate/testdata/huffman-rand-limit.golden", "src/compress/flate/testdata/huffman-rand-limit.in", "src/compress/flate/testdata/huffman-rand-limit.wb.expect", "src/compress/flate/testdata/huffman-rand-limit.wb.expect-noinput", "src/compress/flate/testdata/huffman-rand-max.golden", "src/compress/flate/testdata/huffman-rand-max.in", "src/compress/flate/testdata/huffman-shifts.dyn.expect", "src/compress/flate/testdata/huffman-shifts.dyn.expect-noinput", "src/compress/flate/testdata/huffman-shifts.golden", "src/compress/flate/testdata/huffman-shifts.in", "src/compress/flate/testdata/huffman-shifts.wb.expect", "src/compress/flate/testdata/huffman-shifts.wb.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.dyn.expect", "src/compress/flate/testdata/huffman-text-shift.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text-shift.golden", "src/compress/flate/testdata/huffman-text-shift.in", "src/compress/flate/testdata/huffman-text-shift.wb.expect", "src/compress/flate/testdata/huffman-text-shift.wb.expect-noinput", "src/compress/flate/testdata/huffman-text.dyn.expect", "src/compress/flate/testdata/huffman-text.dyn.expect-noinput", "src/compress/flate/testdata/huffman-text.golden", "src/compress/flate/testdata/huffman-text.in", "src/compress/flate/testdata/huffman-text.wb.expect", "src/compress/flate/testdata/huffman-text.wb.expect-noinput", "src/compress/flate/testdata/huffman-zero.dyn.expect", "src/compress/flate/testdata/huffman-zero.dyn.expect-noinput", "src/compress/flate/testdata/huffman-zero.golden", "src/compress/flate/testdata/huffman-zero.in", "src/compress/flate/testdata/huffman-zero.wb.expect", "src/compress/flate/testdata/huffman-zero.wb.expect-noinput", "src/compress/flate/testdata/null-long-match.dyn.expect-noinput", "src/compress/flate/testdata/null-long-match.wb.expect-noinput", "src/compress/flate/token.go", "src/compress/flate/writer_test.go", "src/compress/gzip/", "src/compress/gzip/example_test.go", "src/compress/gzip/gunzip.go", "src/compress/gzip/gunzip_test.go", "src/compress/gzip/gzip.go", "src/compress/gzip/gzip_test.go", "src/compress/gzip/issue14937_test.go", "src/compress/gzip/testdata/", "src/compress/gzip/testdata/issue6550.gz.base64", "src/compress/lzw/", "src/compress/lzw/reader.go", "src/compress/lzw/reader_test.go", "src/compress/lzw/writer.go", "src/compress/lzw/writer_test.go", "src/compress/testdata/", "src/compress/testdata/e.txt", "src/compress/testdata/gettysburg.txt", "src/compress/testdata/pi.txt", "src/compress/zlib/", "src/compress/zlib/example_test.go", "src/compress/zlib/reader.go", "src/compress/zlib/reader_test.go", "src/compress/zlib/writer.go", "src/compress/zlib/writer_test.go"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}

func Test_metaCacheEntries_filterPrefix(t *testing.T) {
	data := loadMetacacheSampleEntries(t)
	data.filterPrefix("src/compress/bzip2/")
	got := data.entries().names()
	want := []string{"src/compress/bzip2/", "src/compress/bzip2/bit_reader.go", "src/compress/bzip2/bzip2.go", "src/compress/bzip2/bzip2_test.go", "src/compress/bzip2/huffman.go", "src/compress/bzip2/move_to_front.go", "src/compress/bzip2/testdata/", "src/compress/bzip2/testdata/Isaac.Newton-Opticks.txt.bz2", "src/compress/bzip2/testdata/e.txt.bz2", "src/compress/bzip2/testdata/fail-issue5747.bz2", "src/compress/bzip2/testdata/pass-random1.bin", "src/compress/bzip2/testdata/pass-random1.bz2", "src/compress/bzip2/testdata/pass-random2.bin", "src/compress/bzip2/testdata/pass-random2.bz2", "src/compress/bzip2/testdata/pass-sawtooth.bz2", "src/compress/bzip2/testdata/random.data.bz2"}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("got unexpected result: %#v", got)
	}
}
