package communication

import (
	"fmt"
	"net"
	"pkg/models"
)

func SendMovies(conn net.Conn, movies []models.RawMovie) error {
	batch := models.RawMovieBatch{
		Header: models.Header{
			Weight: uint32(len(movies)),
		},
		Movies: movies,
	}

	if err := sendBatch(conn, batch); err != nil {
		return err
	}

	ack, err := recvAck(conn)
	if err != nil {
		return err
	}

	if ack.Read != int(batch.Header.Weight) {
		return fmt.Errorf("error receiving ack: %w", err)
	}

	return nil
}

func SendEOF(conn net.Conn, total int32) error {
	batch := models.RawMovieBatch{
		Header: models.Header{
			TotalWeight: total,
			Weight:      0,
		},
	}

	if err := sendBatch(conn, batch); err != nil {
		return err
	}

	ack, err := recvAck(conn)
	if err != nil {
		return err
	}

	if ack.Read != int(total) {
		return fmt.Errorf("error receiving ack: %w", err)
	}

	return nil
}

func RecvMovies(conn net.Conn) (models.RawMovieBatch, error) {
	batch, err := recvBatch(conn)
	if err != nil {
		return batch, err
	}
	ack := models.Ack{
		Read: int(batch.Header.Weight),
	}

	if batch.Header.TotalWeight > 0 {
		ack.Read = int(batch.Header.TotalWeight)
	}

	if err := sendAck(conn, ack); err != nil {
		return batch, fmt.Errorf("error sending ack: %w", err)
	}

	return batch, nil
}
