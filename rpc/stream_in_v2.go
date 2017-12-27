package rpc

import (
	"context"
	"fmt"
	"io"

	"github.com/pegasus-kv/pegasus-go-client/pegalog"
)

type buffer struct {
	block  []byte
	offset int
	cap    int
}

// next never returns a zero-length slice.
func (buf *buffer) next() []byte {
	if buf.offset == buf.cap {
		// replace with a new block.
		// the old block will not be GC immediately since it's still referenced
		// by the slices holding by the caller.
		buf.block = make([]byte, buf.cap)
		buf.offset = 0
	}

	ret := buf.block[buf.offset:]
	buf.offset = buf.cap
	return ret
}

func (buf *buffer) backup(count int) {
	buf.offset -= count
	if buf.offset < 0 {
		panic(fmt.Sprintf("don't backup with count(%d) larger than offset(%d)", count, buf.offset+count))
	}
}

func newBuffer(cap int) *buffer {
	return &buffer{
		offset: cap, // to trigger allocation of new block when start up.
		cap:    cap,
	}
}

// recvMsg represents the received msg from the transport. All transport
// protocol specific info has been removed.
type recvMsg struct {
	data []byte
	// nil: received some data
	// io.EOF: stream is completed. data is nil.
	// other non-nil error: transport failure. data is nil.
	err error
}

type RpcReadStreamV2 struct {
	reader  io.Reader
	readbuf *buffer

	recvq   chan recvMsg
	nextc   chan recvMsg // channel to reply for the Next request
	reqc    chan int     // channel to request for `size` bytes of data from stream
	pending []recvMsg

	logger pegalog.Logger

	ctx context.Context
}

func (r *RpcReadStreamV2) Next(size int) ([]byte, error) {
	select {
	case r.reqc <- size:
		select {
		case m := <-r.nextc:
			return m.data, m.err
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		}
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}

// Takes out messages sent from LoopForReadFromSocket and handles the
// Next requests. It will breaks the loop whenever an error occurred.
func (r *RpcReadStreamV2) LoopForNext() {
	for {
		select {
		case size := <-r.reqc:
			if len(r.pending) == 0 {
				select {
				case m := <-r.recvq:
					// read from LoopForReadFromSocket if no pending exists.
					r.pending = append(r.pending, m)
				case <-r.ctx.Done():
					break
				}
			}

			m := r.pending[0]
			if len(m.data) > size {
				r.pending[0].data = m.data[:size]
			} else {
				// pop from pending queue
				r.pending[0] = recvMsg{}
				r.pending = r.pending[1:]
			}

			select {
			case r.nextc <- m:
			case <-r.ctx.Done():
				break
			}

			if m.err != nil {
				break
			}
		case m := <-r.recvq:
			r.pending = append(r.pending, m)
		case <-r.ctx.Done():
			break
		}
	}
}

// Loop for reading from io.Reader infinitely until error occurred, then it will
// simply stop looping and return.
// For each of the received data segments, it will send them via recvq to LoopForNext,
// which puts the messages into a pending queue, waiting to be consumed by downstream.
// (caller of Next())
//
// LoopForReadFromSocket  ->  LoopForNext  ->  Next
func (r *RpcReadStreamV2) LoopForReadFromSocket() {
	for {
		// check before starting to read.
		select {
		case <-r.ctx.Done():
			break
		default:
		}

		buf := r.readbuf.next()
		n, err := r.reader.Read(buf)

		if err != nil {
			r.readbuf.backup(len(buf))
		} else {
			r.readbuf.backup(len(buf) - n)
		}

		var m recvMsg
		if n == 0 {
			m = recvMsg{data: nil, err: err}
		} else {
			m = recvMsg{data: buf[:n], err: err}
		}

		select {
		case <-r.ctx.Done():
			break
		case r.recvq <- m:
		}

		if err != nil {
			break
		}
	}
}

func NewRpcReadStreamV2(ctx context.Context, reader io.Reader) *RpcReadStreamV2 {
	r := &RpcReadStreamV2{
		reader:  reader,
		ctx:     ctx,
		logger:  pegalog.GetLogger(),
		readbuf: newBuffer(2 << 14),
		recvq:   make(chan recvMsg, 10),
		nextc:   make(chan recvMsg),
		reqc:    make(chan int),
	}

	go r.LoopForReadFromSocket()
	go r.LoopForNext()
	return r
}
