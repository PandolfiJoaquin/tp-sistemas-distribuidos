package persistency

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"tp-sistemas-distribuidos/server/common"
)

const dataPath = "data/"
const checkpointFileName = "checkpoint.json"
const logFileName = "log.MATADORMATADORMATADORTEESTANBUSCANDO"
const sep = "\x1E"

func LoadCheckpointData() ([]byte, error) {
	//TODO: manejar checkpoints multiples
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

func LoadLogs() ([]TransactionEntry, error) {
	files, err := common.ScanDirectory(dataPath, logFileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(dataPath, 0777); err != nil {
			}
		} else {
			return nil, fmt.Errorf("error scanning directory: %w", err)
		}
	}

	if len(files) == 0 {
		slog.Warn("No log files found")
		return []TransactionEntry{}, nil
	}

	if len(files) > 1 {
		//TODO: manejar logs multiples
		slog.Error("Multiple log files found, using the first one", slog.Any("files", files))
	}

	file := files[0]
	var entries []TransactionEntry
	content, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("error reading log file: %w", err)
	}

	for _, line := range strings.Split(string(content), "\n") {
		entry := entryFromLog(strings.Split(line, sep))
		entries = append(entries, entry)
	}

	return entries, nil
}

func entryFromLog(split []string) TransactionEntry {
	if len(split) < 2 {
		slog.Error("Invalid log entry format", slog.Any("entry", split))
		panic("Invalid log entry format: " + strings.Join(split, sep)) //TODO: sacar
	}

	return TransactionEntry{
		Op:   split[0],
		Args: split[1],
	}
}

func RecoverWithLogs[T any](
	fromBytes func([]byte) (T, error),
	applyFunc func(checkpoint T, entry TransactionEntry) T,
) (T, error) {
	var checkpoint T

	checkpointData, err := LoadCheckpointData()
	if err != nil {
		return checkpoint, fmt.Errorf("error loading checkpoint %v", err)
	}
	checkpoint, err = fromBytes(checkpointData)
	if err != nil {
		return checkpoint, fmt.Errorf("error loading checkpoint %v", err)
	}

	entries, err := LoadLogs()
	if err != nil {
		return checkpoint, fmt.Errorf("error loading logs %v", err)
	}
	checkpoint = applyLogs(checkpoint, entries, applyFunc)
	return checkpoint, nil
}

func applyLogs[T any](checkpoint T, entries []TransactionEntry, applyFunc func(checkpoint T, entry TransactionEntry) T) T {
	for _, entry := range entries {
		checkpoint = applyFunc(checkpoint, entry)
	}
	return checkpoint
}

func SaveCheckpoint(data []byte) error {
	err := os.WriteFile(dataPath+checkpointFileName, data, 0644)
	if err != nil {
		return fmt.Errorf("error writing checkpoint file: %w", err)
	}

	return nil
}
func Commit(transaction Transaction) error {
	f, err := os.OpenFile(dataPath+logFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	defer func(f *os.File) {
		if err := f.Close(); err != nil {
			slog.Error("error closing file", slog.String("file", f.Name()), slog.String("error", err.Error()))
		}
	}(f)

	for _, entry := range transaction.Entries { //TODO: cambiar por funcion de theo
		line := fmt.Sprintf("%s%s%s\n", entry.Op, sep, entry.Args)
		if _, err := f.WriteString(line); err != nil {
			return fmt.Errorf("error writing to log file: %w", err)
		}
	}
	return nil
}

type TransactionEntry struct {
	Op   string
	Args string
}

type Transaction struct {
	Entries []TransactionEntry
}

func (t Transaction) Do(op string, args string) {
	t.Entries = append(t.Entries, TransactionEntry{Op: op, Args: args})
}

func NewTransaction() Transaction {
	return Transaction{Entries: []TransactionEntry{}}
}
