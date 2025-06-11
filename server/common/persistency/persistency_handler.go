package persistency

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"tp-sistemas-distribuidos/server/common"
)

const dataPath = "data/"
const checkpointFileName = "checkpoint-%v.json"
const logFileName = "log.MATADORMATADORMATADORTEESTANBUSCANDO"
const sep = "\x1E"
const commitChar = "c"

var errNoLogFile = errors.New("no log file found")

type Loggable[T any] interface {
	ApplyFunc(entry TransactionEntry) (T, error)
}

type PersistencyHandler[T Loggable[T]] struct {
	currSnapshotNumber int
}

func NewPersistencyHandler[T Loggable[T]]() (*PersistencyHandler[T], error) {
	_, err := getLogPath()
	if err != nil {
		if errors.Is(err, errNoLogFile) {
			checkpointName := dataPath + fmt.Sprintf(checkpointFileName, 0)
			if err := common.AtomicWriteFile(checkpointName, []byte{}, nil); err != nil {
				return nil, fmt.Errorf("error creating checkpoint file: %w", err)
			}
			if err := common.AtomicWriteFile(dataPath+logFileName, fmt.Appendf(nil, "%s\n", checkpointName), nil); err != nil {
				return nil, fmt.Errorf("error creating log file: %w", err)
			}
		} else {
			return nil, fmt.Errorf("error getting log path: %w", err)
		}
	}

	currCheckpointFileName, _, err := getLogsContent(dataPath + logFileName)
	if err != nil {
		return nil, fmt.Errorf("error getting logs content: %w", err)
	}
	currSnapShotNumberString := strings.Split(strings.Split(currCheckpointFileName, "-")[1], ".")[0]
	currSnapshotNumber, err := strconv.Atoi(currSnapShotNumberString)

	if err != nil {
		return nil, fmt.Errorf("error parsing current snapshot number from checkpoint file name: %w", err)
	}

	return &PersistencyHandler[T]{
		currSnapshotNumber: currSnapshotNumber,
	}, nil

}

func (ph *PersistencyHandler[T]) loadCheckpointData(fileName string) ([]byte, error) {
	path := strings.Split(fileName, "/")
	slog.Info("filename", slog.String("fileName", fileName))
	files, err := common.ScanDirectory("./"+path[0], path[1])
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(dataPath, 0777); err != nil {
				return nil, fmt.Errorf("error creating data directory: %w", err)
			}
		} else {
			return nil, fmt.Errorf("error scanning directory: %w", err)
		}
	}

	files = common.Filter(files, func(file string) bool {
		return strings.HasSuffix(file, ".json")
	})

	if len(files) != 1 {
		slog.Warn("there is not 1 checkpoint file", slog.Int("count", len(files)), slog.String("files", strings.Join(files, ", ")))
		panic("there is not 1 checkpoint file, check log for more info") //TODO: sacar
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
			slog.Info("Creating folder")
		} else {
			return "", fmt.Errorf("error scanning directory: %w", err)
		}
	}

	if len(files) == 0 {
		return "", errNoLogFile
	}

	if len(files) > 1 {
		//TODO: manejar logs multiples
		slog.Error("Multiple log files found, using the first one", slog.Any("files", files))
		panic("Multiple log files found") //TODO: sacar
	}

	file := files[0]
	return file, nil
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
		if line == "" {
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

func getLogsContent(logPath string) (string, []string, error) {
	content, err := os.ReadFile(logPath)
	if err != nil {
		return "", nil, fmt.Errorf("error reading log file: %w", err)
	}

	logs := strings.Split(string(content), "\n")
	checkpointFileName := logs[0]
	logs = logs[1:]
	return checkpointFileName, logs, nil
}

func (ph *PersistencyHandler[T]) RecoverFromLogs(fromBytes func([]byte) (T, error)) (T, error) {
	var checkpoint T

	logPath, err := getLogPath()
	if err != nil {
		return checkpoint, fmt.Errorf("error getting log path: %w", err)
	}

	checkpointFileName, logs, err := getLogsContent(logPath)
	if err != nil {
		return checkpoint, fmt.Errorf("error getting logs content: %w", err)
	}

	checkpointData, err := ph.loadCheckpointData(checkpointFileName)
	if err != nil {
		return checkpoint, fmt.Errorf("error loading checkpoint: %v", err)
	}

	checkpoint, err = fromBytes(checkpointData)
	if err != nil {
		return checkpoint, fmt.Errorf("error loading checkpoint: %v", err)
	}

	entries, err := getCommitedLogs(logs)
	if err != nil {
		return checkpoint, fmt.Errorf("error loading logs: %v", err)
	}

	checkpoint, err = applyLogs(checkpoint, entries)
	if err != nil {
		return checkpoint, fmt.Errorf("error applying logs to checkpoint: %w", err)
	}
	return checkpoint, nil
}

func applyLogs[T Loggable[T]](checkpoint T, entries []TransactionEntry) (T, error) {
	var err error
	for _, entry := range entries {
		checkpoint, err = checkpoint.ApplyFunc(entry)
		if err != nil {
			return checkpoint, fmt.Errorf("error applying log entry: %w", err)
		}
	}
	return checkpoint, nil
}

func (ph *PersistencyHandler[T]) SaveCheckpoint(data []byte) error {
	ph.currSnapshotNumber = (ph.currSnapshotNumber + 1) % 2
	checkpointName := dataPath + fmt.Sprintf(checkpointFileName, ph.currSnapshotNumber)
	if err := common.AtomicWriteFile(checkpointName, data, nil); err != nil {
		return fmt.Errorf("error writing checkpoint file: %w", err)
	}

	if err := common.AtomicWriteFile(dataPath+logFileName, fmt.Appendf(nil, "%s\n", checkpointName), nil); err != nil {
		return fmt.Errorf("error writing log file: %w", err)
	}

	_ = os.Remove(dataPath + fmt.Sprintf(checkpointFileName, (ph.currSnapshotNumber+1)%2)) // :)
	return nil
}

func (ph *PersistencyHandler[T]) Commit(transaction Transaction) error {
	if len(transaction.Entries) == 0 {
		return nil
	}
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
