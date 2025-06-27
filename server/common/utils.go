package common

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
)

// ScanDirectory scans a directory and returns all files that have prefix as prefix
func ScanDirectory(dirPath string, prefix string) ([]string, error) {
	var prefixFiles []string

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		filename := filepath.Base(path)

		if strings.HasPrefix(filename, prefix) {
			prefixFiles = append(prefixFiles, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return prefixFiles, nil
}

// GetShard returns the shard number for a given id and the number of shards
func GetShard(id string, shards int) int {
	hash := sha256.Sum256([]byte(id))
	return (int(hash[0]) % shards) + 1
}
