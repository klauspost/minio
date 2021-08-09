// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"hash"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"golang.org/x/crypto/blake2b"
)

var globalMemMetaCache = newMemMetaCache()

type memMetaCacheKey [32]byte

const (
	memMetaCacheMaxMem  = 2 << 30
	memMetaCacheMaxSize = 8 << 10
	memMetaCacheEntries = memMetaCacheMaxMem / (memMetaCacheMaxSize)
)

func newMemMetaCache() *memMetaCache {
	c, err := lru.New2Q(memMetaCacheEntries)
	if err != nil {
		return nil
	}
	return &memMetaCache{data: c}
}

type memMetaCache struct {
	data *lru.TwoQueueCache // Replace with more efficient storage once tested.
}

func (m *memMetaCache) getMetadata(file string) (data []byte, ok bool) {
	if m == nil {
		return nil, false
	}
	if b, ok := m.data.Get(hashFileName(file)); ok {
		return b.([]byte), true
	}
	return nil, false
}

func (m *memMetaCache) setMetadata(file string, data []byte) {
	if m == nil || len(data) > memMetaCacheMaxSize {
		return
	}
	m.data.Add(hashFileName(file), data)
}

func (m *memMetaCache) existsMetadata(file string) (ok bool) {
	if m == nil {
		return false
	}
	return m.data.Contains(hashFileName(file))
}

func (m *memMetaCache) renameMetadata(dst, src string) {
	if m == nil {
		return
	}
	srcH := hashFileName(src)
	dstH := hashFileName(dst)
	b, ok := m.data.Get(srcH)
	if !ok {
		// Be sure we don't keep a stale dst
		m.data.Remove(dstH)
		return
	}
	m.data.Add(dstH, b.(byte))
	m.data.Remove(srcH)
}

var blake2bHasher hash.Hash
var blake2bHasherMu sync.Mutex

func init() {
	var err error
	blake2bHasher, err = blake2b.New256(nil)
	if err != nil {
		panic(err)
	}
}

func hashFileName(s string) (res memMetaCacheKey) {
	// If < 32, just copy and keep rest as zeros.
	if len(s) < 32 {
		copy(res[:], s)
		return
	}
	blake2bHasherMu.Lock()
	blake2bHasher.Reset()
	blake2bHasher.Write([]byte(s))
	// Writes to res...
	blake2bHasher.Sum(res[:0])
	blake2bHasherMu.Unlock()
	return
}
