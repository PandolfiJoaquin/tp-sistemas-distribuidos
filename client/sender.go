package main

import (
	"fmt"
	"log/slog"
	"net"
	"pkg/communication"
	"tp-sistemas-distribuidos/client/utils"
)

type Sender[T any] struct {
	conn      *net.Conn
	dataType  string // for logging
	newReader func(string, int) (utils.BatchReader[T], error)
	path      string
	batchSize int
}

func NewSender[T any](conn *net.Conn, path string, batchSize int, newReader func(string, int) (utils.BatchReader[T], error), dataType string) *Sender[T] {
	return &Sender[T]{
		conn:      conn,
		dataType:  dataType,
		newReader: newReader,
		path:      path,
		batchSize: batchSize,
	}
}

func (s *Sender[T]) Send() error {
	reader, err := s.newReader(s.path, s.batchSize)
	if err != nil {
		return fmt.Errorf("error creating %s reader: %w", s.dataType, err)
	}

	total, err := sendAllData(reader, *s.conn)
	if err != nil {
		return fmt.Errorf("error sending %s: %w", s.dataType, err)
	}

	slog.Info(
		fmt.Sprintf("Sent all %s to server", s.dataType),
		slog.Int("total", total),
	)
	return nil
}

func readAndSendData[T any](reader utils.BatchReader[T], conn net.Conn) error {
	batch, err := reader.ReadBatch()
	if err != nil {
		return fmt.Errorf("error reading batch: %w", err)
	}
	err = communication.SendData[T](conn, batch)
	if err != nil {
		return fmt.Errorf("error sending data: %w", err)
	}
	return nil
}

func sendAllData[T any](reader utils.BatchReader[T], conn net.Conn) (int, error) {
	defer func(reader utils.BatchReader[T]) {
		err := reader.Close()
		if err != nil {
			slog.Error("error closing reader", slog.String("error", err.Error()))
		}
	}(reader)

	for !reader.Finished() {
		err := readAndSendData(reader, conn)
		if err != nil {
			return 0, fmt.Errorf("error sending data: %w", err)
		}
	}

	err := communication.SendBatchEOF(conn, int32(reader.TotalRead()))
	if err != nil {
		return 0, fmt.Errorf("error sending EOF: %w", err)
	}

	return reader.TotalRead(), nil
}
