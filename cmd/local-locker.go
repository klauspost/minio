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

//go:generate msgp -file=$GOFILE -unexported

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/minio/minio/internal/dsync"
	"github.com/puzpuzpuz/xsync/v3"
)

// lockRequesterInfo stores various info from the client for each lock that is requested.
type lockRequesterInfo struct {
	Name            string // name of the resource lock was requested for
	Writer          bool   // Bool whether write or read lock.
	UID             string // UID to uniquely identify request of client.
	Timestamp       int64  // Timestamp set at the time of initialization.
	TimeLastRefresh int64  // Timestamp for last lock refresh.
	Source          string // Contains line, function and filename requesting the lock.
	Group           bool   // indicates if it was a group lock.
	Owner           string // Owner represents the UUID of the owner who originally requested the lock.
	Quorum          int    // Quorum represents the quorum required for this lock to be active.
	idx             int    `msg:"-"` // index of the lock in the lockMap.
}

// isWriteLock returns whether the lock is a write or read lock.
func isWriteLock(lri []lockRequesterInfo) bool {
	return len(lri) == 1 && lri[0].Writer
}

// FIXME: SLICE MUTATIONS ARE NOT SAFE!!!
// MUST BE FIXED BEFORE USE!

// localLocker implements Dsync.NetLocker
//
//msgp:ignore localLocker
type localLocker struct {
	lockMap *xsync.MapOf[string, []lockRequesterInfo]
	// UUID -> resource map.
	lockUID *xsync.MapOf[string, string]
}

func (l *localLocker) String() string {
	return globalEndpoints.Localhost()
}

func (l *localLocker) Lock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	if len(args.Resources) > maxDeleteList {
		return false, fmt.Errorf("internal error: localLocker.Lock called with more than %d resources", maxDeleteList)
	}

	now := UTCNow()

	switch len(args.Resources) {
	case 0:
		return false, fmt.Errorf("internal error: localLocker.Lock called with no resources")
	case 1:
		resource := args.Resources[0]
		l.lockMap.Compute(resource, func(oldValue []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
			reply = !loaded
			if loaded && len(oldValue) > 1 {
				return oldValue, false
			}
			l.lockUID.Store(formatUUID(args.UID, 0), resource)
			return []lockRequesterInfo{
				{
					Name:            resource,
					Writer:          true,
					Source:          args.Source,
					Owner:           args.Owner,
					UID:             args.UID,
					Timestamp:       now.UnixNano(),
					TimeLastRefresh: now.UnixNano(),
					Group:           false,
					Quorum:          *args.Quorum,
					idx:             0,
				},
			}, false
		})
	}

	// No locks held on the all resources, so claim write
	// lock on all resources at once.
	for i, resource := range args.Resources {
		l.lockMap.Compute(resource, func(oldValue []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
			reply = !loaded
			if loaded && len(oldValue) > 1 {
				return oldValue, false
			}
			l.lockUID.Store(formatUUID(args.UID, i), resource)
			return []lockRequesterInfo{
				{
					Name:            resource,
					Writer:          true,
					Source:          args.Source,
					Owner:           args.Owner,
					UID:             args.UID,
					Timestamp:       now.UnixNano(),
					TimeLastRefresh: now.UnixNano(),
					Group:           len(args.Resources) > 1,
					Quorum:          *args.Quorum,
					idx:             i,
				},
			}, false
		})
		if !reply {
			// Revert previous entries and return
			for j, res := range args.Resources[:i] {
				l.lockMap.Delete(res)
				l.lockUID.Delete(formatUUID(args.UID, j))
			}
			return false, nil
		}
	}
	return true, nil
}

func formatUUID(s string, idx int) string {
	return concat(s, strconv.Itoa(idx))
}

func (l *localLocker) Unlock(_ context.Context, args dsync.LockArgs) (reply bool, err error) {
	if len(args.Resources) > maxDeleteList {
		return false, fmt.Errorf("internal error: localLocker.Unlock called with more than %d resources", maxDeleteList)
	}

	err = nil
	for _, resource := range args.Resources {
		l.lockMap.Compute(resource, func(oldValue []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
			if loaded && !isWriteLock(oldValue) {
				err = fmt.Errorf("unlock attempted on a read locked entity: %s", resource)
				return oldValue, false
			}
			if len(oldValue) > 0 {
				// Write locks will only have 1 value..
				l.lockUID.Delete(formatUUID(args.UID, oldValue[0].idx))
			}
			return nil, true
		})
	}
	return true, err
}

// removeEntry based on the uid of the lock message, removes a single entry from the
// lockRequesterInfo array or the whole array from the map (in case of a write lock
// or last read lock)
// UID and optionally owner must match for entries to be deleted.
func (l *localLocker) removeEntryMap(args dsync.LockArgs, lri *[]lockRequesterInfo) (delete bool) {
	// Find correct entry to remove based on uid.
	for index, entry := range *lri {
		if entry.UID == args.UID && (args.Owner == "" || entry.Owner == args.Owner) {
			delete = len(*lri) == 1
			if len(*lri) > 1 {
				// Remove the appropriate read lock.
				*lri = append((*lri)[:index], (*lri)[index+1:]...)
			}
			l.lockUID.Delete(formatUUID(args.UID, entry.idx))
			return delete
		}
	}

	// None found return false, perhaps entry removed in previous run.
	return len(*lri) == 0
}

func (l *localLocker) RLock(_ context.Context, args dsync.LockArgs) (reply bool, err error) {
	if len(args.Resources) != 1 {
		return false, fmt.Errorf("internal error: localLocker.RLock called with more than one resource")
	}

	// l.mutex.Lock()
	// defer l.mutex.Unlock()
	resource := args.Resources[0]
	now := UTCNow()
	lrInfo := lockRequesterInfo{
		Name:            resource,
		Writer:          false,
		Source:          args.Source,
		Owner:           args.Owner,
		UID:             args.UID,
		Timestamp:       now.UnixNano(),
		TimeLastRefresh: now.UnixNano(),
		Quorum:          *args.Quorum,
	}
	reply = true
	l.lockMap.Compute(resource, func(oldValue []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
		if !loaded {
			// New entry, add
			l.lockUID.Store(formatUUID(args.UID, lrInfo.idx), resource)
			return []lockRequesterInfo{lrInfo}, false
		}
		if reply = !isWriteLock(oldValue); reply {
			// Existing entry, read lock
			l.lockUID.Store(formatUUID(args.UID, lrInfo.idx), resource)
			return append(oldValue, lrInfo), false
		}
		return oldValue, false
	})
	return reply, nil
}

func (l *localLocker) RUnlock(_ context.Context, args dsync.LockArgs) (reply bool, err error) {
	if len(args.Resources) > 1 {
		return false, fmt.Errorf("internal error: localLocker.RUnlock called with more than one resource")
	}

	resource := args.Resources[0]
	l.lockMap.Compute(resource, func(lri []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
		if !loaded {
			return nil, true
		}
		if isWriteLock(lri) {
			// A write-lock is held, cannot release a read lock
			reply = false
			err = fmt.Errorf("RUnlock attempted on a write locked entity: %s", resource)
			return lri, false
		}
		return lri, l.removeEntryMap(args, &lri)
	})
	return err == nil, err
}

type lockStats struct {
	Total  int
	Writes int
	Reads  int
}

func (l *localLocker) stats() lockStats {
	st := lockStats{Total: l.lockMap.Size()}
	l.lockMap.Range(func(key string, v []lockRequesterInfo) bool {
		if len(v) == 0 {
			return true
		}
		entry := v[0]
		if entry.Writer {
			st.Writes++
		} else {
			st.Reads += len(v)
		}
		return true
	})
	return st
}

type localLockMap map[string][]lockRequesterInfo

func (l *localLocker) DupLockMap() localLockMap {
	lockCopy := make(map[string][]lockRequesterInfo, l.lockMap.Size())
	l.lockMap.Range(func(key string, v []lockRequesterInfo) bool {
		l.lockMap.Compute(key, func(oldValue []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
			if loaded {
				lockCopy[key] = append(make([]lockRequesterInfo, 0, len(v)), v...)
			}
			return newValue, !loaded
		})
		return true
	})
	return lockCopy
}

func (l *localLocker) Close() error {
	return nil
}

// IsOnline - local locker is always online.
func (l *localLocker) IsOnline() bool {
	return true
}

// IsLocal - local locker returns true.
func (l *localLocker) IsLocal() bool {
	return true
}

func (l *localLocker) ForceUnlock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	if err = ctx.Err(); err != nil {
		return false, err
	}

	if len(args.UID) == 0 {
		for _, resource := range args.Resources {
			l.lockMap.Compute(resource, func(lris []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
				if !loaded || len(lris) == 0 {
					// No entry...
					return nil, true
				}
				uids := make([]string, 0, len(lris))
				for _, lri := range lris {
					uids = append(uids, lri.UID)
				}
				// Delete collected uids:
				for _, uid := range uids {
					// Just to be safe, delete uuids.
					for idx := 0; idx < maxDeleteList; idx++ {
						if _, ok := l.lockUID.LoadAndDelete(formatUUID(uid, idx)); !ok {
							break
						}
					}
					delete = l.removeEntryMap(dsync.LockArgs{UID: uid}, &lris)
				}
				return lris, delete
			})
		}
		return true, nil
	}

	for idx := 0; idx < maxDeleteList; idx++ {
		// Walk through all indices until we don'
		mapID := formatUUID(args.UID, idx)
		resource, loaded := l.lockUID.Load(mapID)
		if !loaded {
			return idx > 0, nil
		}
		l.lockMap.Compute(resource, func(lris []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
			l.lockUID.Delete(mapID)
			if !loaded {
				return nil, true
			}
			return lris, l.removeEntryMap(dsync.LockArgs{UID: args.UID}, &lris)
		})
		reply = true
	}
	return true, nil
}

func (l *localLocker) Refresh(ctx context.Context, args dsync.LockArgs) (refreshed bool, err error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	// Check whether uid is still active.
	resource, ok := l.lockUID.Load(formatUUID(args.UID, 0))
	if !ok {
		return false, nil
	}

	for idx := 0; idx < maxDeleteList; idx++ {
		l.lockMap.Compute(resource, func(lris []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
			if !loaded || len(lris) == 0 {
				// Inconsistent. Delete UID.
				l.lockUID.Delete(formatUUID(args.UID, idx))
				return nil, true
			}
			now := UTCNow()
			for i := range lris {
				if lris[i].UID == args.UID {
					// Copy
					lri := lris[i]
					lri.TimeLastRefresh = now.UnixNano()
					lris[i] = lri
				}
			}
			return lris, false
		})
		_, ok = l.lockUID.Load(formatUUID(args.UID, idx+1))
		if !ok {
			// No more resources for UID, but we did update at least one.
			return true, nil
		}
	}
	return true, nil
}

// Similar to removeEntry but only removes an entry only if the lock entry exists in map.
// Caller must hold 'l.mutex' lock.
func (l *localLocker) expireOldLocks(interval time.Duration) {
	l.lockMap.Range(func(key string, lris []lockRequesterInfo) bool {
		var anyExpired bool
		for i := 0; i < len(lris); {
			lri := &lris[i]
			if time.Since(time.Unix(0, lri.TimeLastRefresh)) > interval {
				anyExpired = true
				break
			}
		}
		if !anyExpired {
			return true
		}
		l.lockMap.Compute(key, func(lris []lockRequesterInfo, loaded bool) (newValue []lockRequesterInfo, delete bool) {
			for i := 0; i < len(lris); {
				lri := &lris[i]
				if time.Since(time.Unix(0, lri.TimeLastRefresh)) > interval {
					delete = l.removeEntryMap(dsync.LockArgs{UID: lri.UID}, &lris)
					if delete {
						return nil, true
					}
				}
			}
			return lris, len(lris) == 0
		})
		return true
	})
}

func newLocker() *localLocker {
	return &localLocker{
		lockMap: xsync.NewMapOf[string, []lockRequesterInfo](xsync.WithPresize(1000)),
		lockUID: xsync.NewMapOf[string, string](xsync.WithPresize(1000)),
	}
}
