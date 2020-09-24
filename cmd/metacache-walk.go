package cmd

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/logger"
)

// WalkDirOptions provides options for WalkDir operations.
type WalkDirOptions struct {
	Bucket    string
	BaseDir   string
	Recursive bool
}

// WalkDir will traverse a directory and return all entries found.
// On success a meta cache stream will be returned.
func (s *xlStorage) WalkDir(ctx context.Context, opts WalkDirOptions) (io.ReadCloser, error) {
	atomic.AddInt32(&s.activeIOCount, 1)
	defer func() {
		atomic.AddInt32(&s.activeIOCount, -1)
	}()

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(opts.Bucket)
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
	if HasSuffix(opts.BaseDir, SlashSeparator) {
		if st, err := os.Stat(pathJoin(volumeDir, opts.BaseDir, xlStorageFormatFile)); err == nil && st.Mode().IsRegular() {
			return nil, errFileNotFound
		}
	}

	scanDirs := []string{opts.BaseDir}
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

		entries, err := s.ListDir(ctx, opts.Bucket, current, -1)
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
					if opts.Recursive {
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
	sorted := res.sort()
	// Stream output.
	r, w := io.Pipe()
	go sorted.writeTo(w)
	return r, nil
}

func (p *xlStorageDiskIDCheck) WalkDir(ctx context.Context, opts WalkDirOptions) (io.ReadCloser, error) {
	if err := p.checkDiskStale(); err != nil {
		return nil, err
	}
	return p.storage.WalkDir(ctx, opts)
}

// WalkDir will traverse a directory and return all entries found.
// On success a meta cache stream will be returned, that should be closed when done.
func (client *storageRESTClient) WalkDir(ctx context.Context, opts WalkDirOptions) (io.ReadCloser, error) {
	values := make(url.Values)
	values.Set(storageRESTVolume, opts.BaseDir)
	values.Set(storageRESTFilePath, opts.Bucket)
	values.Set(storageRESTRecursive, strconv.FormatBool(opts.Recursive))
	respBody, err := client.call(ctx, storageRESTMethodWalkDir, values, nil, -1)
	if err != nil {
		return nil, err
	}
	return waitForHTTPResponseCloser(respBody)
}

// WalkDirHandler - remote caller to list files and folders in a requested directory path.
func (s *storageRESTServer) WalkDirHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsValid(w, r) {
		return
	}
	vars := mux.Vars(r)
	volume := vars[storageRESTVolume]
	dirPath := vars[storageRESTDirPath]
	recursive, err := strconv.ParseBool(vars[storageRESTRecursive])
	done := keepHTTPResponseAlive(w)
	if err != nil {
		done(err)
		return
	}

	resp, err := s.storage.WalkDir(r.Context(), WalkDirOptions{Bucket: volume, BaseDir: dirPath, Recursive: recursive})
	done(err)
	if err == nil {
		// Copy response stream
		defer resp.Close()
		io.Copy(w, resp)
	}
}
