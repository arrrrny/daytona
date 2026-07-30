// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package pty

import (
	"log/slog"
	"time"

	"github.com/google/uuid"
	cmap "github.com/orcaman/concurrent-map/v2"
)

// NewEphemeralPTYSession creates a PTY session that is NOT registered with
// the global PTY manager: it is owned and torn down by the caller (e.g. the
// exec-over-WebSocket controller) and is invisible to the PTY REST endpoints.
// It reuses the exact same start/read/write/resize/kill machinery as
// manager-registered sessions.
func NewEphemeralPTYSession(logger *slog.Logger, info PTYSessionInfo) *PTYSession {
	if info.ID == "" {
		info.ID = uuid.NewString()
	}
	if info.CreatedAt.IsZero() {
		info.CreatedAt = time.Now()
	}

	return &PTYSession{
		info:    info,
		clients: cmap.New[*wsClient](),
		outSubs: cmap.New[chan []byte](),
		done:    make(chan struct{}),
		logger:  logger.With(slog.String("sessionId", info.ID)),
	}
}

// Start launches the PTY process (same semantics as CreatePTYSession with
// LazyStart=false).
func (s *PTYSession) Start() error {
	return s.start()
}

// SubscribeOutput registers a channel that receives the same raw PTY output
// broadcast as attached WebSocket clients. The returned unsubscribe function
// detaches the subscriber. A subscriber whose buffer stays full is dropped
// (its channel is closed), mirroring the slow-consumer policy for WebSocket
// clients.
func (s *PTYSession) SubscribeOutput(buffer int) (<-chan []byte, func()) {
	if buffer <= 0 {
		buffer = 256
	}
	ch := make(chan []byte, buffer)
	key := uuid.NewString()

	s.outSubs.Set(key, ch)

	return ch, func() {
		s.outSubs.Remove(key)
	}
}

// WriteInput writes raw bytes to the PTY (stdin).
func (s *PTYSession) WriteInput(data []byte) error {
	return s.sendToPTY(data)
}

// Resize changes the PTY window size (TIOCSWINSZ).
func (s *PTYSession) Resize(cols, rows uint16) error {
	return s.resize(cols, rows)
}

// Kill terminates the PTY session and its whole process tree.
func (s *PTYSession) Kill() {
	s.kill()
}

// WaitExit blocks until the PTY process has exited and returns its exit code
// (128+signal when killed by a signal, e.g. 130 for SIGINT).
func (s *PTYSession) WaitExit() int {
	<-s.done
	return s.exitCode
}

// ShellPid returns the PID of the PTY's shell process, or 0 if not running.
// The PTY child is a session leader, so its process group ID equals its PID.
func (s *PTYSession) ShellPid() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil && s.cmd.Process != nil {
		return s.cmd.Process.Pid
	}
	return 0
}
