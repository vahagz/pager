package pager

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPager(t *testing.T) {
	filename := "test.bin"
	require.NoError(t, os.Remove(filename))
	defer os.Remove(filename)

	p, err := Open(filename, os.Getpagesize(), false, 0644)
	require.NoError(t, err)

	id, err := p.Alloc(1)
	require.NoError(t, err)
	require.Equal(t, uint64(0), id)

	data := []byte{0,1,2,3,4,5,6,7,8,9}
	require.NoError(t, p.Write(id, data))

	readData, err := p.Read(id)
	require.NoError(t, err)
	require.Equal(t, 0, bytes.Compare(data, readData[:len(data)]))
}
