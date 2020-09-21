package cmd

import (
	"bytes"
	"io"
	"sort"
	"strings"
)

type metaCacheObject struct {
	// name is the full name of the object including prefixes
	name string
	// Metadata. If none is present it is not an object but only a prefix.
	// Entries without metadata will only be present in non-recursive scans.
	metadata []byte
}

type metaCacheObjects []metaCacheObject

// metaCacheObjectsSorted can be used
type metaCacheObjectsSorted struct {
	o metaCacheObjects
}

// less function for sorting.
func (m metaCacheObjects) less(i, j int) bool {
	return m[i].name < m[j].name
}

// isDir returns if the object is representing a prefix directory.
func (o metaCacheObject) isDir() bool {
	return len(o.metadata) == 0
}

// sort entries by name.
// m is sorted and a sorted metadata object is returned.
// Changes to m will also be reflected in the returned object.
func (m metaCacheObjects) sort() metaCacheObjectsSorted {
	if m.isSorted() {
		return metaCacheObjectsSorted{o: m}
	}
	sort.Slice(m, m.less)
	return metaCacheObjectsSorted{o: m}
}

// isSorted returns whether the objects are sorted.
// This is usually orders of magnitude faster than actually sorting.
func (m metaCacheObjects) isSorted() bool {
	return sort.SliceIsSorted(m, m.less)
}

func (m metaCacheObjectsSorted) WriteTo(writer io.Writer) error {
	w := newMetacacheStream(writer)
	if err := w.write(m.o...); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// forwardTo will truncate m so only entries that are s or after is in the list.
func (m *metaCacheObjectsSorted) forwardTo(s string) {
	if s == "" {
		return
	}
	idx := sort.Search(len(m.o), func(i int) bool {
		return m.o[i].name >= s
	})
	m.o = m.o[idx:]
}

// filterPrefix will filter m to only contain entries with the specified prefix.
func (m *metaCacheObjectsSorted) filterPrefix(s string) {
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
func (m *metaCacheObjectsSorted) objectsOnly() {
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
func (m *metaCacheObjectsSorted) prefixesOnly() {
	dst := m.o[:0]
	for _, o := range m.o {
		if o.isDir() {
			dst = append(dst, o)
		}
	}
	m.o = dst
}

// len returns the number of objects and prefix dirs in m.
func (m *metaCacheObjectsSorted) len() int {
	if m == nil {
		return 0
	}
	return len(m.o)
}

// deduplicate entries in the list.
// If compareMeta is set it will be used to resolve conflicts.
// The function should return whether the existing entry should be replaced with other.
// If no compareMeta is provided duplicates may be left.
// This is indicated by the returned boolean.
func (m *metaCacheObjectsSorted) deduplicate(compareMeta func(existing, other []byte) (replace bool)) (dupesLeft bool) {
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
