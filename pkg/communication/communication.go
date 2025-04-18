package communication

import (
	"encoding/json"
	"fmt"
	"net"
	"pkg/models"
)

func sendBatch(conn net.Conn, batch models.RawMovieBatch) error {
	enc := json.NewEncoder(conn)
	if err := enc.Encode(batch); err != nil {
		return fmt.Errorf("error sending batch: %w", err)
	}
	return nil
}

func sendAck(conn net.Conn, ack models.Ack) error {
	enc := json.NewEncoder(conn)
	if err := enc.Encode(ack); err != nil {
		return fmt.Errorf("error sending ack: %w", err)
	}
	return nil
}

func recvBatch(conn net.Conn) (models.RawMovieBatch, error) {
	dec := json.NewDecoder(conn)
	var batch models.RawMovieBatch
	if err := dec.Decode(&batch); err != nil {
		return batch, fmt.Errorf("error receiving batch: %w", err)
	}
	return batch, nil
}

func recvAck(conn net.Conn) (models.Ack, error) {
	dec := json.NewDecoder(conn)
	var ack models.Ack
	if err := dec.Decode(&ack); err != nil {
		return ack, fmt.Errorf("error receiving ack: %w", err)
	}
	return ack, nil
}
