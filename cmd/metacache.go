/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"path"
	"strings"
	"time"
)

type scanStatus uint8

const (
	scanStateNone scanStatus = iota
	scanStateStarted
	scanStateSuccess
	scanStateError
)

//go:generate msgp -file $GOFILE -unexported

type metacache struct {
	id           string     `msg:"id"`
	bucket       string     `msg:"b"`
	root         string     `msg:"root"`
	recursive    bool       `msg:"rec"`
	status       scanStatus `msg:"stat"`
	error        string     `msg:"err"`
	totalObjects string     `msg:"to"`
	started      time.Time  `msg:"st"`
	ended        time.Time  `msg:"end"`
	startedCycle uint64     `msg:"stc"`
	endedCycle   uint64     `msg:"endc"`
	dataVersion  uint8      `msg:"v"`
}

func loadMetaCache(ctx context.Context, bucket string) (*metacache, error) {
	return nil, nil
}

func baseDirFromPrefix(prefix string) string {
	b := path.Dir(prefix)
	if b == "." || b == "./" {
		b = ""
	}
	if len(b) > 0 && !strings.HasSuffix(b, slashSeparator) {
		b += slashSeparator
	}
	return b
}
