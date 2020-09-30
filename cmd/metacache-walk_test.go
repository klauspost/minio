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
	"fmt"
	"os"
	"testing"
)

func Test_xlStorage_WalkDir(t *testing.T) {
	//t.Skip("Manual Testing Only")
	xl, err := newLocalXLStorage("d:\\data\\mindev\\data2\\xl1\\")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create("file.out")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	//err = xl.WalkDir(context.Background(), WalkDirOptions{Bucket: "mybucket", BaseDir: "src/compress", Recursive: false}, f)
	err = xl.WalkDir(context.Background(), WalkDirOptions{Bucket: "warp-benchmark-bucket", BaseDir: "", Recursive: true}, f)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_xlStorage_WalkVersions(t *testing.T) {
	t.Skip("Manual Testing Only")
	xl, err := newLocalXLStorage("d:\\data\\mindev\\data2\\xl1\\")
	if err != nil {
		t.Fatal(err)
	}

	results, err := xl.WalkVersions(context.Background(), "warp-benchmark-bucket", "", "", true, nil)
	//results, err := xl.WalkDir(context.Background(), "mybucket", "", true)
	if err != nil {
		t.Fatal(err)
	}
	wr := newMetacacheFile("file.out")
	defer wr.Close()
	var n int
	for _ = range results {
		//t.Log("got result: ", res.name)
		n++
	}
	fmt.Println("got", n)
}
