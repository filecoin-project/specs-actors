package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Expected exactly one argument, path of directory to digest")
		os.Exit(1)
	}
	rootDir := os.Args[1]
	data := make([]byte, 0)
	err := filepath.Walk(rootDir+"/", func(path string, info os.FileInfo, err error) error {
		data = append(data, []byte(path)...)
		return nil
	})
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
	h := sha256.Sum256(data)
	fmt.Printf("- %x\n", h)
}
