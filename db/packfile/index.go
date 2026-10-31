package packfile

import (
	"io"

	"github.com/aperturerobotics/go-kvfile"
	"github.com/pkg/errors"
)

// ReadIndex reads the key index of a packfile of size bytes from ra, without
// its block values. The entries are in key order.
func ReadIndex(ra io.ReaderAt, size uint64) ([]*kvfile.IndexEntry, error) {
	// Read the index at the tail of the packfile.
	_, tail, err := kvfile.ReadIndexTail(ra, size)
	if err != nil {
		return nil, errors.Wrap(err, "read packfile index")
	}

	// Collect its entries.
	var index []*kvfile.IndexEntry
	err = ScanIndexTail(tail, size, func(ie *kvfile.IndexEntry) error {
		index = append(index, ie.CloneVT())
		return nil
	})
	return index, err
}

// IndexSuffixSize returns the bytes at the end of a packfile of size bytes that
// hold the index tail of a pack of blockCount blocks, whatever the encoding
// of its keys. One read of that many bytes ends at the end of the object.
func IndexSuffixSize(blockCount, size uint64) (uint64, error) {
	maxTail, err := kvfile.MaxIndexTailSize(blockCount)
	if err != nil {
		return 0, err
	}
	return min(maxTail, size), nil
}

// ScanIndexSuffix calls fn with each index entry of a packfile of size bytes
// in key order, given the last bytes of the packfile as suffix, which hold at
// least its index tail. fn must copy what it keeps of the entry.
func ScanIndexSuffix(suffix []byte, size uint64, fn func(*kvfile.IndexEntry) error) error {
	_, tail, err := kvfile.TrimIndexTail(suffix, size)
	if err != nil {
		return errors.Wrap(err, "trim packfile index")
	}
	return ScanIndexTail(tail, size, fn)
}

// ScanIndexTail calls fn with each index entry of the packfile of size bytes
// whose index tail is tail, in key order, without collecting them. fn must copy
// what it keeps of the entry.
func ScanIndexTail(tail []byte, size uint64, fn func(*kvfile.IndexEntry) error) error {
	rd, err := kvfile.BuildReaderWithIndexTail(tail, size)
	if err != nil {
		return errors.Wrap(err, "open packfile index")
	}
	err = rd.ScanPrefixEntries(nil, func(ie *kvfile.IndexEntry, _ int) error {
		return fn(ie)
	})
	if err != nil {
		return errors.Wrap(err, "scan packfile index")
	}
	return nil
}
