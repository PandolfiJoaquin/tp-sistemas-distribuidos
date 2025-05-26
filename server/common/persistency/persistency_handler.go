package persistency

import (
	"errors"
	"fmt"
	"os"
	"tp-sistemas-distribuidos/server/common"
)

const dataPath = "data/"
const checkpointFileName = "checkpoint.json"

func Recover() ([]byte, error) {

	files, err := common.ScanDirectory(dataPath, checkpointFileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(dataPath, 0777); err != nil {
				return nil, fmt.Errorf("error creating data directory: %w", err)
			}
		} else {
			return nil, fmt.Errorf("error scanning directory: %w", err)
		}
	}

	if len(files) == 0 {
		return []byte{}, nil
	}

	file := files[0]

	content, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("error reading checkpoint file: %w", err)
	}

	return content, nil
}

func SaveCheckpoint(data []byte) error {
	err := os.WriteFile(dataPath+checkpointFileName, data, 0644)
	if err != nil {
		return fmt.Errorf("error writing checkpoint file: %w", err)
	}

	return nil
}

// func NewPersistencyHandler(basePath string) *PersistencyHandler {
// 	return &PersistencyHandler{basePath: basePath}
// }

// func (p *PersistencyHandler) SaveTransaction(data string) error {
// 	return nil
// }

/*
logs.txt
xxxxxxxxxxxxx
add movie: xxx xxx xxx xxx
add review: xxxxx
add movie: xxxxx
*/
