package main

import (
	"fmt"
	"encoding"
)

type transactionLogEntry[T any] struct {
	fnName T
	args []encoding.TextMarshaler
}

type TransactionLog[T any] struct {
	ops []transactionLogEntry[T]
	runFnFromName func(T, ...encoding.TextMarshaler) error
}

func NewTransactionLog[T any](switchFn func(T, ...encoding.TextMarshaler) error) *TransactionLog[T] {
	return &TransactionLog[T]{
		ops:      make([]transactionLogEntry[T], 0),
		runFnFromName: switchFn,
	}
}

func (t *TransactionLog[T]) Add(fnName T, args ...encoding.TextMarshaler) error {
	if err := t.runFnFromName(fnName, args...); err != nil {
		return fmt.Errorf("error running function %v: %w", fnName, err)
	}
	t.ops = append(t.ops, transactionLogEntry[T]{
		fnName: fnName,
		args: args,
	})
	return nil
}
