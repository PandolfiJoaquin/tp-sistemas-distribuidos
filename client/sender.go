package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"pkg/communication"
	"time"
	"tp-sistemas-distribuidos/client/utils"
)

// Monke: 2.5
// Joaco: 20.0
const msToSleep = 20.0

type Sender[T any] struct {
	conn       *net.Conn
	dataType   string // for logging
	newReader  func(string, int) (utils.BatchReader[T], error)
	path       string
	batchSize  int
	signalCtx  context.Context
	ctx        context.Context
	ackChannel <-chan int
}

func NewSender[T any](conn *net.Conn, path string, batchSize int, newReader func(string, int) (utils.BatchReader[T], error), dataType string, ackChannel <-chan int, signalCtx context.Context, ctx context.Context) *Sender[T] {
	return &Sender[T]{
		conn:       conn,
		dataType:   dataType,
		newReader:  newReader,
		path:       path,
		batchSize:  batchSize,
		signalCtx:  signalCtx,
		ctx:        ctx,
		ackChannel: ackChannel,
	}
}

func (s *Sender[T]) Send() error {
	reader, err := s.newReader(s.path, s.batchSize)
	if err != nil {
		return fmt.Errorf("error creating %s reader: %w", s.dataType, err)
	}

	total, err := s.sendAllData(reader)
	if err != nil {
		return fmt.Errorf("error sending %s: %w", s.dataType, err)
	}

	slog.Info(
		fmt.Sprintf("Sent all %s to server", s.dataType),
		slog.Int("total", total),
	)
	return nil
}

func (s *Sender[T]) SendBatch(batch []T, total int, last bool) error {
	var err error
	if last {
		slog.Debug("Sending EOF batch", slog.Any("type", s.dataType), slog.Any("header", batch))
		err = communication.SendBatchEOF(*s.conn, int32(total))
	} else {
		err = communication.SendData(*s.conn, batch)
	}

	if err != nil {
		return fmt.Errorf("error sending %s batch: %w", s.dataType, err)
	}

	select {
	case <-s.signalCtx.Done():
		slog.Info("Context done, stopping sending data")
	case <-s.ctx.Done():
	case ack := <-s.ackChannel:
		if batch == nil && ack != 0 {
			return fmt.Errorf("eof ack value mismatch: expected 0, got %d", ack)
		} else if batch != nil && ack != len(batch) {
			return fmt.Errorf("ack value mismatch: expected %d, got %d", len(batch), ack)
		}
	}

	time.Sleep(time.Duration(msToSleep) * time.Millisecond)
	return nil
}

func (s *Sender[T]) sendAllData(reader utils.BatchReader[T]) (int, error) {
	defer func(reader utils.BatchReader[T]) {
		err := reader.Close()
		if err != nil {
			slog.Error("error closing reader", slog.String("error", err.Error()))
		}
	}(reader)

	for !reader.Finished() {
		batch, err := reader.ReadBatch()
		if err != nil {
			return -1, fmt.Errorf("error reading batch: %w", err)
		}
		if batch == nil {
			slog.Debug("Received nil batch, stopping sending data")
			break
		}
		err = s.SendBatch(batch, -1, false)
		if err != nil {
			return -1, fmt.Errorf("error sending batch : %w", err)
		}
	}

	// Send EOF
	slog.Debug("Sending EOF In sendAllData", slog.Any("type", s.dataType))
	err := s.SendBatch(nil, reader.TotalRead(), true)
	if err != nil {
		return 0, fmt.Errorf("error sending EOF: %w", err)
	}

	return reader.TotalRead(), nil
}
