package pager

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var bin = binary.BigEndian

const disableMmap = false

// InMemoryFileName can be passed to Open() to create a pager for an ephemeral
// in-memory file.
const InMemoryFileName = ":memory:"

// ErrReadOnly is returned when a write operation is attempted on a read-only
// pager instance.
var ErrReadOnly = errors.New("read-only")

// Open opens the named file and returns a pager instance for it. If the file
// doesn't exist, it will be created if not in read-only mode.
func Open(fileName string, blockSz int, mode os.FileMode) (*Pager, error) {
	if fileName == InMemoryFileName {
		return newPager(&inMemory{}, fileName, blockSz)
	}

	flag := os.O_CREATE | os.O_RDWR

	f, err := os.OpenFile(fileName, flag, mode)
	if err != nil {
		return nil, err
	}

	return newPager(f, fileName, blockSz)
}

// newPager creates an instance of pager for given random access file object.
// By default page size is set to the current system page size.
func newPager(file RandomAccessFile, fileName string, pageSize int) (*Pager, error) {
	size, err := findSize(file)
	if err != nil {
		return nil, err
	}

	osFile, _ := file.(*os.File)

	p := &Pager{
		file:     file,
		fileName: fileName,
		fileSize: size,
		pageSize: pageSize,
		osFile:   osFile,
	}
	p.computeCount()

	return p, nil
}

// Pager provides facilities for paged I/O on file-like objects with random
// access. If the underlying file is os.File type, memory mapping will be
// enabled when file size is non-zero.
type Pager struct {
	// internal states
	file     RandomAccessFile
	fileName string
	pageSize int
	fileSize int64
	count    uint64
	readOnly bool

	// memory mapping state for os.File
	osFile *os.File

	// i/o tracking
	writes int
	reads  int
	allocs int
}

// Alloc allocates 'n' new sequential pages and returns the id of the first
// page in sequence.
func (p *Pager) Alloc(n int) (uint64, error) {
	if p.file == nil {
		return 0, os.ErrClosed
	} else if p.readOnly {
		return 0, ErrReadOnly
	}

	nextID := p.count

	targetSize := p.fileSize + int64(n*p.pageSize)
	if err := p.file.Truncate(targetSize); err != nil {
		return 0, err
	}

	p.fileSize = targetSize
	p.computeCount()

	p.allocs++
	return nextID, nil
}

// Free deallocates 'n' sequential pages from end of file
func (p *Pager) Free(n int) error {
	if p.file == nil {
		return os.ErrClosed
	} else if p.readOnly {
		return ErrReadOnly
	}

	if n > int(p.count) {
		n = int(p.count)
	}
	targetSize := p.fileSize - int64(n*p.pageSize)

	if err := p.file.Truncate(targetSize); err != nil {
		return err
	}

	p.fileSize = targetSize
	p.computeCount()

	return nil
}

// Read reads one page of data from the underlying file or mmapped region if
// enabled.
func (p *Pager) Read(id uint64) ([]byte, error) {
	if id < 0 || id >= p.count {
		return nil, fmt.Errorf("invalid page id=%d (max=%d)", id, p.count-1)
	} else if p.file == nil {
		return nil, os.ErrClosed
	}

	buf := make([]byte, p.pageSize)

	n, err := p.file.ReadAt(buf, p.offset(id))
	if n < p.pageSize {
		return nil, io.EOF
	}
	p.reads++
	return buf, err
}

// ReadAt reads length count of bytes starting from offset
func (p *Pager) ReadAt(dst []byte, offset uint64) error {
	if offset + uint64(len(dst)) > uint64(p.fileSize) {
		return fmt.Errorf("invalid file offset (filesize=%d, offset=%d)", p.fileSize, offset)
	} else if p.file == nil {
		return os.ErrClosed
	}

	n, err := p.file.ReadAt(dst, int64(offset))
	if n < len(dst) {
		return io.EOF
	}
	if err != nil {
		return err
	}
	p.reads++
	return nil
}

// Write writes one page of data to the page with given id. Returns error if
// the data is larger than a page.
func (p *Pager) Write(id uint64, d []byte) error {
	if id < 0 || id >= p.count {
		return fmt.Errorf("invalid page id=%d (max=%d)", id, p.count-1)
	} else if len(d) > p.pageSize {
		return errors.New("data is larger than a page")
	} else if p.file == nil {
		return os.ErrClosed
	} else if p.readOnly {
		return ErrReadOnly
	}

	_, err := p.file.WriteAt(d, p.offset(id))
	if err != nil {
		return err
	}
	p.writes++
	return nil
}

// WriteAt writes length count of bytes starting from offset
func (p *Pager) WriteAt(src []byte, offset uint64) error {
	if offset + uint64(len(src)) > uint64(p.fileSize) {
		return fmt.Errorf("invalid file offset (filesize=%d, offset=%d)", p.fileSize, offset)
	} else if p.file == nil {
		return os.ErrClosed
	} else if p.readOnly {
		return ErrReadOnly
	}

	n, err := p.file.WriteAt(src, int64(offset))
	if n < len(src) {
		return io.EOF
	}
	if err != nil {
		return err
	}
	p.writes++
	return nil
}

// Marshal writes the marshaled value of 'v' into page with given id.
func (p *Pager) Marshal(id uint64, v encoding.BinaryMarshaler) error {
	d, err := v.MarshalBinary()
	if err != nil {
		return err
	}
	return p.Write(id, d)
}

// Unmarshal reads the page with given id and unmarshals the page data using
// 'into' and 'slot'.
func (p *Pager) Unmarshal(id uint64, into encoding.BinaryUnmarshaler) error {
	d, err := p.Read(id)
	if err != nil {
		return err
	}
	return into.UnmarshalBinary(d)
}

// PageSize returns the size of one page used by pager.
func (p *Pager) PageSize() int { return p.pageSize }

// Count returns the number of pages in the underlying file. Returns error if
// the file is closed.
func (p *Pager) Count() uint64 { return p.count }

// ReadOnly returns true if the pager instance is in read-only mode.
func (p *Pager) ReadOnly() bool { return p.readOnly }

func (p *Pager) Remove() {
	p.file.Close()
	os.Remove(p.fileName)
}

// Close closes the underlying file and marks the pager as closed for use.
func (p *Pager) Close() error {
	if p.file == nil {
		return nil
	}

	err := p.file.Close()
	p.osFile = nil
	p.file = nil
	return err
}

// Stats returns i/o stats collected by this pager.
func (p *Pager) Stats() Stats {
	return Stats{
		Allocs: p.allocs,
		Reads:  p.reads,
		Writes: p.writes,
	}
}

func (p *Pager) String() string {
	if p.file == nil {
		return fmt.Sprintf("Pager{closed=true}")
	}

	return fmt.Sprintf(
		"Pager{file='%s', readOnly=%t, pageSize=%d, count=%d}",
		p.file.Name(), p.readOnly, p.pageSize, p.count,
	)
}

func (p *Pager) computeCount() {
	p.count = uint64(p.fileSize) / uint64(p.pageSize)
}

func (p *Pager) offset(id uint64) int64 {
	return int64(uint64(p.pageSize) * id)
}

// Stats represents I/O statistics collected by the pager.
type Stats struct {
	Writes int
	Reads  int
	Allocs int
}

func (s Stats) String() string {
	return fmt.Sprintf(
		"Stats{writes=%d, allocs=%d, reads=%d}",
		s.Writes, s.Allocs, s.Reads,
	)
}
