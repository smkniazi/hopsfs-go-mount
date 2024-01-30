// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

// Implements ReadSeekCloser interface with automatic retries (acts as a proxy to HdfsReader)
type FaultTolerantHdfsReader struct {
	Path         string
	Impl         ReadSeekCloser
	HdfsAccessor HdfsAccessor
	RetryPolicy  *RetryPolicy
	Offset       int64
}

var _ ReadSeekCloser = (*FaultTolerantHdfsReader)(nil) // ensure FaultTolerantHdfsReaderImpl implements ReadSeekCloser
// Creates new instance of FaultTolerantHdfsReader
func NewFaultTolerantHdfsReader(path string, impl ReadSeekCloser, hdfsAccessor HdfsAccessor, retryPolicy *RetryPolicy) *FaultTolerantHdfsReader {
	return &FaultTolerantHdfsReader{Path: path, Impl: impl, HdfsAccessor: hdfsAccessor, RetryPolicy: retryPolicy}
}

// Read a chunk of data
func (ftr *FaultTolerantHdfsReader) Read(buffer []byte) (int, error) {
	op := ftr.RetryPolicy.StartOperation()
	for {
		var err error
		if ftr.Impl == nil {
			// Re-opening the file for read
			ftr.Impl, err = ftr.HdfsAccessor.OpenRead(ftr.Path)
			if err != nil {
				if op.ShouldRetry("[%s] OpenRead: %s", ftr.Path, err.Error()) {
					continue
				} else {
					return 0, err
				}
			}
			// Seeking to the right offset
			if err = ftr.Impl.Seek(ftr.Offset); err != nil {
				// Those errors are non-recoverable propagating right away
				ftr.Close()
				return 0, err
			}
		}
		// Performing the read
		var nr int
		nr, err = ftr.Impl.Read(buffer)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("[%s] Read @%d: %s", ftr.Path, ftr.Offset, err.Error()) {
			if err == nil {
				// On successful read, adjusting offset to the actual number of bytes read
				ftr.Offset += int64(nr)
			}
			return nr, err
		}
		// On failure, we need to close the reader
		ftr.Close()
	}
}

// Seeks to a given position
func (ftr *FaultTolerantHdfsReader) Seek(pos int64) error {
	// Seek is implemented as virtual operation on which doesn't involve communication,
	// passing that through without retires and promptly propagate errors
	// (which will be non-recoverable in this case)
	err := ftr.Impl.Seek(pos)
	if err == nil {
		// On success, updating current readng position
		ftr.Offset = pos
	}
	return err
}

// Returns current position
func (ftr *FaultTolerantHdfsReader) Position() (int64, error) {
	// This fault-tolerant wrapper keeps track the position on its own, no need
	// to query the backend
	return ftr.Offset, nil
}

// Closes the stream
func (ftr *FaultTolerantHdfsReader) Close() error {
	err := ftr.Impl.Close()
	ftr.Impl = nil
	return err
}
