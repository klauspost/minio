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
