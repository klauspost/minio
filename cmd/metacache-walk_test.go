package cmd

import (
	"context"
	"fmt"
	"testing"
)

func Test_xlStorage_WalkDir(t *testing.T) {
	t.Skip("Manual Testing Only")
	xl, err := newLocalXLStorage("d:\\data\\mindev\\data2\\xl1\\")
	if err != nil {
		t.Fatal(err)
	}
	//results, err := xl.WalkDir(context.Background(), "warp-benchmark-bucket", "", true)
	results, err := xl.WalkDir(context.Background(), WalkDirOptions{Bucket: "mybucket", BaseDir: "src/compress", Recursive: true})
	if err != nil {
		t.Fatal(err)
	}
	wr := newMetacacheFile("file.out")
	defer wr.Close()
	err = wr.write(results.entries()...)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("got", results.len())
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
