package record

import (
	"compress/gzip"
	"encoding/binary"
	"github.com/tursom/GoCollections/exceptions"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"strings"
)

func ReadRecord(path string, callback func(record *RecordMsg) error) error {
	file, err := os.Open(path)
	if err != nil {
		return exceptions.Package(err)
	}
	defer func(file *os.File) {
		exceptions.Print(file.Close())
	}(file)

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return exceptions.Package(err)
	}

	sizeBuffer := make([]byte, 4)
	read, err := gzReader.Read(sizeBuffer)
	if err != nil {
		return exceptions.Package(err)
	}

	for read == 4 {
		var size = binary.BigEndian.Uint32(sizeBuffer)
		buffer := make([]byte, size)
		for size != 0 {
			gzRead, err := gzReader.Read(buffer[len(buffer)-int(size):])
			if err != nil && err != io.EOF {
				return exceptions.Package(err)
			}
			size -= uint32(gzRead)
			if err == io.EOF && size != 0 {
				return exceptions.Package(err)
			}
		}

		recordMsg := &RecordMsg{}
		err = proto.Unmarshal(buffer, recordMsg)
		if err != nil {
			return exceptions.Package(err)
		}
		err = callback(recordMsg)
		if err != nil {
			return exceptions.Package(err)
		}

		read, err = gzReader.Read(sizeBuffer)
		if err == io.EOF {
			break
		} else if err != nil {
			return exceptions.Package(err)
		}
	}
	return nil
}

func LoopRecordFile(path string, callback func(path string) error) error {
	files, err := os.ReadDir(path)
	if err != nil {
		return exceptions.Package(err)
	}

	for _, f := range files {
		path := f.Name()
		if !strings.HasSuffix(path, ".rec") {
			continue
		}
		err = callback(path)
		if err != nil {
			return exceptions.Package(err)
		}
	}
	return nil
}
