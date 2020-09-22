package cmd

import (
	"bytes"
	"io"
	"sort"
	"strings"
)

// metaCacheEntry is an object or prefix within an unknown bucket.
type metaCacheEntry struct {
	// name is the full name of the object including prefixes
	name string
	// Metadata. If none is present it is not an object but only a prefix.
	// Entries without metadata will only be present in non-recursive scans.
	metadata []byte
}

// metaCacheEntries is a slice of metacache entries.
type metaCacheEntries []metaCacheEntry

// less function for sorting.
func (m metaCacheEntries) less(i, j int) bool {
	return m[i].name < m[j].name
}

// isDir returns if the object is representing a prefix directory.
func (o metaCacheEntry) isDir() bool {
	return len(o.metadata) == 0
}

// sort entries by name.
// m is sorted and a sorted metadata object is returned.
// Changes to m will also be reflected in the returned object.
func (m metaCacheEntries) sort() metaCacheEntriesSorted {
	if m.isSorted() {
		return metaCacheEntriesSorted{o: m}
	}
	sort.Slice(m, m.less)
	return metaCacheEntriesSorted{o: m}
}

// isSorted returns whether the objects are sorted.
// This is usually orders of magnitude faster than actually sorting.
func (m metaCacheEntries) isSorted() bool {
	return sort.SliceIsSorted(m, m.less)
}

// shallowClone will create a shallow clone of the array objects,
// but object metadata will not be cloned.
func (m metaCacheEntries) shallowClone() metaCacheEntries {
	dst := make(metaCacheEntries, len(m))
	for i, obj := range m {
		dst[i] = obj
	}
	return dst
}

// names will return all names in order.
// Since this allocates it should not be used in critical functions.
func (m metaCacheEntries) names() []string {
	res := make([]string, 0, len(m))
	for _, obj := range m {
		res = append(res, obj.name)
	}
	return res
}

// metaCacheEntriesSorted contains metacache entries that are sorted.
type metaCacheEntriesSorted struct {
	o metaCacheEntries
}

// writeTo will write all objects to the provided output.
func (m metaCacheEntriesSorted) writeTo(writer io.Writer) error {
	w := newMetacacheWriter(writer)
	if err := w.write(m.o...); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// shallowClone will create a shallow clone of the array objects,
// but object metadata will not be cloned.
func (m metaCacheEntriesSorted) shallowClone() metaCacheEntriesSorted {
	// We have value receiver so we already have a copy.
	m.o = m.o.shallowClone()
	return m
}

func (m *metaCacheEntriesSorted) iterate(fn func(entry metaCacheEntry) (cont bool)) {
	if m == nil {
		return
	}
	for _, o := range m.o {
		if !fn(o) {
			return
		}
	}
}

// forwardTo will truncate m so only entries that are s or after is in the list.
func (m *metaCacheEntriesSorted) forwardTo(s string) {
	if s == "" {
		return
	}
	idx := sort.Search(len(m.o), func(i int) bool {
		return m.o[i].name >= s
	})
	m.o = m.o[idx:]
}

// merge will merge other into m.
// If the same entries exists in both the entries from m will be placed first.
// Operation time is expected to be O(n+m).
func (m *metaCacheEntriesSorted) merge(other metaCacheEntriesSorted) {
	merged := make(metaCacheEntries, 0, m.len()+other.len())
	a := m.entries()
	b := other.entries()
	for len(a) > 0 && len(b) > 0 {
		if a[0].name <= b[0].name {
			merged = append(merged, a[0])
			a = a[1:]
		} else {
			merged = append(merged, b[0])
			b = b[1:]
		}
	}
	// Append anything left.
	merged = append(merged, a...)
	merged = append(merged, b...)
	m.o = merged
}

// filterPrefix will filter m to only contain entries with the specified prefix.
func (m *metaCacheEntriesSorted) filterPrefix(s string) {
	if s == "" {
		return
	}
	m.forwardTo(s)
	for i, o := range m.o {
		if !strings.HasPrefix(o.name, s) {
			m.o = m.o[:i]
			break
		}
	}
}

// objectsOnly will remove prefix directories.
// Order is preserved, but the underlying slice is modified.
func (m *metaCacheEntriesSorted) objectsOnly() {
	dst := m.o[:0]
	for _, o := range m.o {
		if !o.isDir() {
			dst = append(dst, o)
		}
	}
	m.o = dst
}

// objectsOnly will remove prefix directories.
// Order is preserved, but the underlying slice is modified.
func (m *metaCacheEntriesSorted) prefixesOnly() {
	dst := m.o[:0]
	for _, o := range m.o {
		if o.isDir() {
			dst = append(dst, o)
		}
	}
	m.o = dst
}

// len returns the number of objects and prefix dirs in m.
func (m *metaCacheEntriesSorted) len() int {
	if m == nil {
		return 0
	}
	return len(m.o)
}

// entries returns the underlying objects as is currently represented.
func (m *metaCacheEntriesSorted) entries() metaCacheEntries {
	if m == nil {
		return nil
	}
	return m.o
}

// deduplicate entries in the list.
// If compareMeta is set it will be used to resolve conflicts.
// The function should return whether the existing entry should be replaced with other.
// If no compareMeta is provided duplicates may be left.
// This is indicated by the returned boolean.
func (m *metaCacheEntriesSorted) deduplicate(compareMeta func(existing, other []byte) (replace bool)) (dupesLeft bool) {
	dst := m.o[:0]
	for _, obj := range m.o {
		found := false
		for i := len(dst) - 1; i >= 0; i++ {
			existing := dst[i]
			if existing.name != obj.name {
				break
			}
			if bytes.Equal(obj.metadata, existing.metadata) {
				found = true
				break
			}
			if compareMeta != nil {
				if compareMeta(existing.metadata, obj.metadata) {
					dst[i] = obj
				}
				found = true
				break
			}

			// Matches, move on.
			dupesLeft = true
			continue
		}
		if !found {
			dst = append(dst, obj)
		}
	}
	m.o = dst
	return dupesLeft
}
