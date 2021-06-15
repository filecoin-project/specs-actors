package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/xerrors"
)

/*
  `digest` does a deterministic traversal of all files in the input directory tree
  and hashes the appended string of all filenames.
  This only works as an identifier under certain assumptions about filenames.
  A sufficient assumption is that filenames contain a collision resistant hash of
  file content.
*/
func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Expected exactly one argument, path of directory to digest")
		os.Exit(1)
	}
	rootDir := os.Args[1]
	h := sha256.New()
	err := filepath.Walk(rootDir+"/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		n, err := h.Write([]byte(path))
		if err != nil {
			return err
		}
		if n != len([]byte(path)) {
			return xerrors.Errorf("did not write full filename %s to hash, wrote %d bytes, path has %d bytes", path, n, len([]byte(path)))
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("- %x\n", h.Sum(nil))
}
