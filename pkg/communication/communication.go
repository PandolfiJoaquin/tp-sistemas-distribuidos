package communication

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"pkg/models"
)
const (
	MaxPacketSize = 1024 * 32 // 32 KB
	size = 4
)

func RecvAll(conn net.Conn, size int) ([]byte, error) {
	/// Read all the bytes from the connection to avoid partial reads
	buf := make([]byte, size)
	total := 0

	for total < size {
		n, err := conn.Read(buf[total:])
		if err != nil || n == 0 {
			return nil, fmt.Errorf("error reading from connection: %w", err)
		}
		total += n
	}
	return buf, nil
}

func SendAll(conn net.Conn, message []byte) error {
	/// send all the bytes to the connection to avoid partial writes
	written := 0
	for written < len(message) {
		n, err := conn.Write(message)
		if err != nil || n == 0 {
			return fmt.Errorf("error writing to connection: %w", err)
		}
		written += n
	}
	return nil
}

func sendFixedSize(conn net.Conn, data []byte) error {
	sizeBuf := make([]byte, size)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(data)))
	dataBin := append(sizeBuf, data...)

	if err := SendAll(conn, dataBin); err != nil {
		return err
	}
	return nil
}



func sendBatch(conn net.Conn, batch models.RawBatch) error {
    data, err := json.Marshal(batch)
    if err != nil {
        return fmt.Errorf("error marshalling batch: %w", err)
    }

	// Adding the header
    header := make([]byte, size)
    binary.BigEndian.PutUint32(header, uint32(len(data)))
    current := append([]byte(nil), header...)

    for offset := 0; offset < len(data); offset += MaxPacketSize{
        end := offset + MaxPacketSize 
        if end > len(data) {
            end = len(data)
        }
        chunk := data[offset:end]
		// Sends current if its size + chunk size is greater than MaxPacketSize
        if len(current)+len(chunk) > MaxPacketSize{
            if err := SendAll(conn, current); err != nil {
                return fmt.Errorf("error sending batch: %w", err)
            }
			// reset
            current = []byte{}
        }

        current = append(current, chunk...)
    }

    if len(current) > 0 {
        if err := SendAll(conn, current); err != nil {
            return fmt.Errorf("error sending final batch: %w", err)
        }
    }

    return nil
}



// func sendBatch(conn net.Conn, batch models.RawBatch) error {
// 	data, err := json.Marshal(batch)
// 	if err != nil {
// 		return fmt.Errorf("error marshalling batch: %w", err)
// 	}
// 	err = sendFixedSize(conn, data)
// 	if err != nil {
// 		return fmt.Errorf("error sending batch: %w", err)
// 	}
// 	return nil
// }

func recvMovieBatch(conn net.Conn) (models.RawMovieBatch, error) {
	var batch models.RawMovieBatch

	sizeBuf, err := RecvAll(conn, size)
	if err != nil {
		return batch, fmt.Errorf("error reading size: %w", err)
	}

	size := binary.BigEndian.Uint32(sizeBuf)

	// Read the actual message
	dataBuf, err := RecvAll(conn, int(size))
	if err != nil {
		return batch, fmt.Errorf("error reading data: %w", err)
	}

	if err := json.Unmarshal(dataBuf, &batch); err != nil {
		return batch, fmt.Errorf("error unmarshalling batch: %w", err)
	}

	return batch, nil
}

func recvReviewBatch(conn net.Conn) (models.RawReviewBatch, error) {
	var batch models.RawReviewBatch

	sizeBuf, err := RecvAll(conn, size)
	if err != nil {
		return batch, fmt.Errorf("error reading size: %w", err)
	}

	size := binary.BigEndian.Uint32(sizeBuf)

	// Read the actual message
	dataBuf, err := RecvAll(conn, int(size))
	if err != nil {
		return batch, fmt.Errorf("error reading data: %w", err)
	}

	if err := json.Unmarshal(dataBuf, &batch); err != nil {
		return batch, fmt.Errorf("error unmarshalling batch: %w", err)
	}

	return batch, nil
}

func recvCreditBatch(conn net.Conn) (models.RawCreditBatch, error) {
	var batch models.RawCreditBatch

	sizeBuf, err := RecvAll(conn, size)
	if err != nil {
		return batch, fmt.Errorf("error reading size: %w", err)
	}

	size := binary.BigEndian.Uint32(sizeBuf)

	// Read the actual message
	dataBuf, err := RecvAll(conn, int(size))
	if err != nil {
		return batch, fmt.Errorf("error reading data: %w", err)
	}

	if err := json.Unmarshal(dataBuf, &batch); err != nil {
		return batch, fmt.Errorf("error unmarshalling batch: %w", err)
	}

	return batch, nil
}

func sendResults(conn net.Conn, results models.Results) error {
	data, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("error marshalling results: %w", err)
	}

	err = sendFixedSize(conn, data)
	if err != nil {
		return fmt.Errorf("error sending results: %w", err)
	}

	return nil
}

func recvResults(conn net.Conn) (models.Results, error) {
	var results models.Results

	sizeBuf, err := RecvAll(conn, size)
	if err != nil {
		return results, fmt.Errorf("error reading size: %w", err)
	}

	size := binary.BigEndian.Uint32(sizeBuf)

	// Read the actual message
	dataBuf, err := RecvAll(conn, int(size))
	if err != nil {
		return results, fmt.Errorf("error reading data: %w", err)
	}

	if err := json.Unmarshal(dataBuf, &results); err != nil {
		return results, fmt.Errorf("error unmarshalling results: %w", err)
	}

	return results, nil
}
