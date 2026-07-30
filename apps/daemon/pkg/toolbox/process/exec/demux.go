// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package exec

import (
	"bytes"

	"github.com/daytonaio/common-go/pkg/log"
)

type streamKind int

const (
	streamStdout streamKind = iota
	streamStderr
)

// streamDemux incrementally demultiplexes the command wrapper's labeled
// output (see cmdWrapperFormat: every line is prefixed with STDOUT_PREFIX or
// STDERR_PREFIX) into per-stream content. It mirrors session.DemuxLogBytes
// but works on streaming chunks: a marker split across chunk boundaries is
// held back until the next Write or Flush.
type streamDemux struct {
	emit    func(kind streamKind, data []byte)
	current streamKind
	pending []byte // trailing bytes that may be the start of a split marker
}

func newStreamDemux(emit func(kind streamKind, data []byte)) *streamDemux {
	return &streamDemux{emit: emit, current: streamStdout}
}

func (d *streamDemux) Write(chunk []byte) {
	if len(chunk) == 0 {
		return
	}

	buf := make([]byte, 0, len(d.pending)+len(chunk))
	buf = append(buf, d.pending...)
	buf = append(buf, chunk...)
	d.pending = nil

	segStart := 0
	i := 0
	for i < len(buf) {
		kind, isMarker, isPartial := matchMarkerAt(buf, i)
		switch {
		case isMarker:
			d.emitRange(buf[segStart:i])
			d.current = kind
			i += len(log.STDOUT_PREFIX)
			segStart = i
		case isPartial:
			// Tail of the buffer may be a marker split across chunks — keep
			// it in pending and emit everything before it.
			d.emitRange(buf[segStart:i])
			d.pending = append(d.pending, buf[i:]...)
			return
		default:
			i++
		}
	}
	d.emitRange(buf[segStart:])
}

// Flush emits any held-back partial marker bytes as regular content. It must
// be called when the stream is known to be complete (exit code written).
func (d *streamDemux) Flush() {
	if len(d.pending) > 0 {
		d.emitRange(d.pending)
		d.pending = nil
	}
}

func (d *streamDemux) emitRange(data []byte) {
	if len(data) == 0 {
		return
	}
	d.emit(d.current, data)
}

// matchMarkerAt reports whether buf[i:] starts with a complete stream marker
// (isMarker), or whether it is a proper prefix of a marker at the tail of the
// buffer (isPartial) and should be held back for the next chunk.
func matchMarkerAt(buf []byte, i int) (kind streamKind, isMarker, isPartial bool) {
	rest := buf[i:]
	if bytes.HasPrefix(rest, log.STDOUT_PREFIX) {
		return streamStdout, true, false
	}
	if bytes.HasPrefix(rest, log.STDERR_PREFIX) {
		return streamStderr, true, false
	}
	if len(rest) < len(log.STDOUT_PREFIX) {
		if bytes.HasPrefix(log.STDOUT_PREFIX, rest) || bytes.HasPrefix(log.STDERR_PREFIX, rest) {
			return streamStdout, false, true
		}
	}
	return streamStdout, false, false
}
