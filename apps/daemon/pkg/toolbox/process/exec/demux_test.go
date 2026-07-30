// Copyright Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package exec

import (
	"strings"
	"testing"

	"github.com/daytonaio/common-go/pkg/log"
)

type demuxCapture struct {
	stdout strings.Builder
	stderr strings.Builder
}

func (c *demuxCapture) emit(kind streamKind, data []byte) {
	if kind == streamStderr {
		c.stderr.Write(data)
	} else {
		c.stdout.Write(data)
	}
}

func prefixed(prefix, s string) string {
	return prefix + s + "\n"
}

func TestStreamDemuxBasic(t *testing.T) {
	capture := &demuxCapture{}
	d := newStreamDemux(capture.emit)

	input := prefixed(string(log.STDOUT_PREFIX), "hello") +
		prefixed(string(log.STDERR_PREFIX), "oops") +
		prefixed(string(log.STDOUT_PREFIX), "world")

	d.Write([]byte(input))
	d.Flush()

	if got := capture.stdout.String(); got != "hello\nworld\n" {
		t.Fatalf("unexpected stdout %q", got)
	}
	if got := capture.stderr.String(); got != "oops\n" {
		t.Fatalf("unexpected stderr %q", got)
	}
}

func TestStreamDemuxSplitMarkerAcrossChunks(t *testing.T) {
	capture := &demuxCapture{}
	d := newStreamDemux(capture.emit)

	full := prefixed(string(log.STDOUT_PREFIX), "one") + string(log.STDERR_PREFIX) + "two\n"

	// Feed byte by byte — every possible marker split is exercised.
	for i := 0; i < len(full); i++ {
		d.Write([]byte{full[i]})
	}
	d.Flush()

	if got := capture.stdout.String(); got != "one\n" {
		t.Fatalf("unexpected stdout %q", got)
	}
	if got := capture.stderr.String(); got != "two\n" {
		t.Fatalf("unexpected stderr %q", got)
	}
}

func TestStreamDemuxIncompleteMarkerAtEOF(t *testing.T) {
	capture := &demuxCapture{}
	d := newStreamDemux(capture.emit)

	// A trailing marker prefix that never completes must be emitted as
	// regular stream content on Flush, not discarded or treated as a marker.
	prefix := string(log.STDOUT_PREFIX)
	d.Write([]byte("content" + prefix[:len(prefix)-1]))
	d.Flush()

	if got := capture.stdout.String(); got != "content"+prefix[:len(prefix)-1] {
		t.Fatalf("unexpected stdout %q", got)
	}
	if got := capture.stderr.String(); got != "" {
		t.Fatalf("unexpected stderr %q", got)
	}
}

func TestStreamDemuxMatchesReferenceDemux(t *testing.T) {
	capture := &demuxCapture{}
	d := newStreamDemux(capture.emit)

	chunks := []string{
		prefixed(string(log.STDOUT_PREFIX), "line one"),
		prefixed(string(log.STDOUT_PREFIX), "line two"),
		prefixed(string(log.STDERR_PREFIX), "error line"),
		prefixed(string(log.STDOUT_PREFIX), "line three"),
	}
	var full string
	for _, c := range chunks {
		full += c
	}

	// Write in awkward chunk sizes.
	const chunkSize = 7
	for i := 0; i < len(full); i += chunkSize {
		end := i + chunkSize
		if end > len(full) {
			end = len(full)
		}
		d.Write([]byte(full[i:end]))
	}
	d.Flush()

	wantStdout := "line one\nline two\nline three\n"
	wantStderr := "error line\n"

	if got := capture.stdout.String(); got != wantStdout {
		t.Fatalf("stdout %q, want %q", got, wantStdout)
	}
	if got := capture.stderr.String(); got != wantStderr {
		t.Fatalf("stderr %q, want %q", got, wantStderr)
	}
}
