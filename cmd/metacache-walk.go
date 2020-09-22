package cmd

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"sync/atomic"

	"github.com/minio/minio/cmd/logger"
)

func (s *xlStorage) WalkDir(ctx context.Context, bucket, dirPath string, recursive bool) (metaCacheEntries, error) {
	atomic.AddInt32(&s.activeIOCount, 1)
	defer func() {
		atomic.AddInt32(&s.activeIOCount, -1)
	}()

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(bucket)
	if err != nil {
		return nil, err
	}

	// Stat a volume entry.
	_, err = os.Stat(volumeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errVolumeNotFound
		} else if isSysErrIO(err) {
			return nil, errFaultyDisk
		}
		return nil, err
	}

	// Fast exit track to check if we are listing an object with
	// a trailing slash, this will avoid to list the object content.
	if HasSuffix(dirPath, SlashSeparator) {
		if st, err := os.Stat(pathJoin(volumeDir, dirPath, xlStorageFormatFile)); err == nil && st.Mode().IsRegular() {
			return nil, errFileNotFound
		}
	}

	scanDirs := []string{dirPath}
	var res metaCacheEntries
	for len(scanDirs) > 0 {
		current := scanDirs[0]

		// Trim front entry from scanDirs.
		if len(scanDirs) > 1 {
			// Copy to front
			copy(scanDirs, scanDirs[1:])
			scanDirs = scanDirs[:len(scanDirs)-1]
		} else {
			scanDirs = scanDirs[:0]
		}

		entries, err := s.ListDir(ctx, bucket, current, -1)
		if err != nil {
			// Folder could have gone away in-between
			if err != errVolumeNotFound {
				logger.LogIf(ctx, err)
			}
			continue
		}

		// Pre-alloc what we know we will need
		if len(res) == 0 {
			res = make(metaCacheEntries, 0, len(entries))
		}
		for _, entry := range entries {
			// All objects will be returned as directories, there has been no object check yet.
			meta := metaCacheEntry{name: PathJoin(current, entry)}
			if HasSuffix(meta.name, SlashSeparator) {
				meta.metadata, err = ioutil.ReadFile(pathJoin(volumeDir, meta.name, xlStorageFormatFile))
				switch {
				case err == nil:
					meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
					res = append(res, meta)
					continue
				case os.IsNotExist(err):
					if recursive {
						scanDirs = append(scanDirs, meta.name)
					}
					res = append(res, meta)
					continue
				default:
					logger.LogIf(ctx, err)
					continue
				}
			} else {
				if HasSuffix(entry, xlStorageFormatFile) {
					meta.metadata, err = ioutil.ReadFile(pathJoin(volumeDir, meta.name, xlStorageFormatFile))
					if err != nil {
						logger.LogIf(ctx, err)
						continue
					}
					meta.name = strings.TrimSuffix(meta.name, xlStorageFormatFile)
					meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
					res = append(res, meta)
					continue
				}
				// Ignore files that are not metadata
			}
		}
	}

	return res, nil
}
