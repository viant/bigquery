package reader

import (
	"bytes"
	"compress/gzip"
	"io"
)

// Gzip reads data from the passed reader and returns a buffer with compressed data
func Gzip(reader io.Reader) (io.Reader, error) {
	writerBuf := new(bytes.Buffer)
	writer := gzip.NewWriter(writerBuf)
	_, err := io.Copy(writer, reader)

	if err == nil {
		err := writer.Flush()
		if err != nil {
			return nil, err
		}
		err = writer.Close()
		if err != nil {
			return nil, err
		}
		return writerBuf, nil
	}
	return nil, err
}
