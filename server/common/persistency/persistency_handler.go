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
const checkpointFileName = "checkpoint-%v.json"
const logFileName = "log.MATADORMATADORMATADORTEESTANBUSCANDO"
const sep = "\x1E"
const commitChar = "c"

func LoadCheckpointData(fileName string) ([]byte, error) {
	files, err := common.ScanDirectory(dataPath, fileName)
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
		slog.Warn("No checkpoint files found")
		//panic("No checkpoint files found") //TODO: sacar
		return []byte{}, nil
	}

	file := files[0]

	content, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("error reading checkpoint file: %w", err)
	}

	return content, nil
}

func getLogPath() (string, error) {
	files, err := common.ScanDirectory(dataPath, logFileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(dataPath, 0777); err != nil {
				return "", fmt.Errorf("error creating data directory: %w", err)
			}
		} else {
			return "", fmt.Errorf("error scanning directory: %w", err)
		}
	}

	if len(files) == 0 {
		checkpointName := dataPath + fmt.Sprintf(checkpointFileName, 1) 
		if err := common.AtomicWriteFile(checkpointName, []byte{}, nil); err != nil {
			return "", fmt.Errorf("error creating checkpoint file: %w", err)
		}
		if err := common.AtomicWriteFile(dataPath+logFileName, fmt.Appendf(nil, "%s\n", checkpointName), nil); err != nil {
			return "", fmt.Errorf("error creating log file: %w", err)
		}
		return dataPath + logFileName, nil
	}

	if len(files) > 1 {
		//TODO: manejar logs multiples
		slog.Error("Multiple log files found, using the first one", slog.Any("files", files))
		panic("Multiple log files found") //TODO: sacar
	}

	file := files[0]
	return dataPath + file, nil
}

func getCommitedLogs(logs []string) ([]TransactionEntry, error) {
	var entries []TransactionEntry
	var uncommitedEntries []TransactionEntry
	for _, line := range logs {
		if line == commitChar {
			entries = append(entries, uncommitedEntries...)
			uncommitedEntries = []TransactionEntry{}
			continue
		}
		entry := entryFromLog(strings.Split(line, sep))
		uncommitedEntries = append(uncommitedEntries, entry)
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
	//TODO: Esta funcion la chequeamos y ta bien, falta borrar los logs (ver casos bordes) y hacer seguimiento de las otras funciones
	var checkpoint T

	logPath, err := getLogPath()
	if err != nil {
		return checkpoint, fmt.Errorf("error getting log path: %w", err)
	}

	content, err := os.ReadFile(logPath)
	if err != nil {
		return checkpoint, fmt.Errorf("error reading log file: %w", err)
	}

	logs := strings.Split(string(content), "\n")
	checkpointFileName := logs[0]
	logs = logs[1:]

	checkpointData, err := LoadCheckpointData(checkpointFileName)
	if err != nil {
		return checkpoint, fmt.Errorf("error loading checkpoint %v", err)
	}

	checkpoint, err = fromBytes(checkpointData)
	if err != nil {
		return checkpoint, fmt.Errorf("error loading checkpoint %v", err)
	}

	entries, err := getCommitedLogs(logs)
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
	checkpointName := dataPath + fmt.Sprintf(checkpointFileName, 1)
	if err := common.AtomicWriteFile(checkpointName, data, nil); err != nil {
		return fmt.Errorf("error writing checkpoint file: %w", err)
	}
	return nil
}

func Commit(transaction Transaction) error {
	//TODO: optimizacion: Abrir 1 sola vez el archivo y escribir en el
	var lines string
	for _, entry := range transaction.Entries {
		line := fmt.Sprintf("%s%s%s\n", entry.Op, sep, entry.Args)
		lines += line
	}
	lines += fmt.Sprintf("%s\n", commitChar)
	if err := common.AppendLine(dataPath+logFileName, []byte(lines)); err != nil {
		return fmt.Errorf("error appending to log file: %w", err)
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

func (t *Transaction) Do(op string, args string) {
	t.Entries = append(t.Entries, TransactionEntry{Op: op, Args: args})
}

func NewTransaction() Transaction {
	return Transaction{Entries: []TransactionEntry{}}
}
