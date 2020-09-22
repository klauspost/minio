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
