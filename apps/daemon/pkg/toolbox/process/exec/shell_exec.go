// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package exec

import (
	"context"
	"fmt"
	"log/slog"
	"syscall"
	"time"

	"github.com/daytonaio/daemon/pkg/toolbox/process/pty"
	"github.com/google/uuid"
)

const (
	defaultShellCols = 80
	defaultShellRows = 24

	// exitDrainTimeout bounds how long buffered PTY output is flushed after
	// the shell process has exited, before the exit frame is emitted.
	exitDrainTimeout = 250 * time.Millisecond
)

// shellSignalControlChars maps signals to their terminal control characters;
// writing them to the PTY master makes the line discipline deliver the signal
// to the foreground process group — exactly like a terminal would.
var shellSignalControlChars = map[syscall.Signal]byte{
	syscall.SIGINT:  0x03, // ^C (INTR)
	syscall.SIGQUIT: 0x1c, // ^\ (QUIT)
	syscall.SIGTSTP: 0x1a, // ^Z (TSTP)
}

// shellSession is an interactive login shell backed by a PTY (the same
// machinery as the toolbox PTY endpoints): a real terminal with job control,
// so signals arrive as control characters and resize maps to TIOCSWINSZ.
type shellSession struct {
	logger  *slog.Logger
	workDir string
	session *pty.PTYSession
}

func newShellSession(logger *slog.Logger, workDir string) *shellSession {
	return &shellSession{
		logger:  logger,
		workDir: workDir,
	}
}

func (s *shellSession) Start(ctx context.Context, start StartFrame, emit func(frame any, final bool)) error {
	cwd := start.Cwd
	if cwd == "" {
		cwd = s.workDir
	}

	envs := make(map[string]string, len(start.Env)+1)
	for k, v := range start.Env {
		envs[k] = v
	}
	if envs["TERM"] == "" {
		envs["TERM"] = "xterm-256color"
	}

	cols := start.Cols
	if cols == 0 {
		cols = defaultShellCols
	}
	rows := start.Rows
	if rows == 0 {
		rows = defaultShellRows
	}

	s.session = pty.NewEphemeralPTYSession(s.logger, pty.PTYSessionInfo{
		ID:        "exec-" + uuid.NewString(),
		Cwd:       cwd,
		Envs:      envs,
		Cols:      cols,
		Rows:      rows,
		CreatedAt: time.Now(),
	})

	out, unsubscribe := s.session.SubscribeOutput(256)

	if err := s.session.Start(); err != nil {
		unsubscribe()
		return fmt.Errorf("failed to start shell: %w", err)
	}

	go s.forward(ctx, out, emit)
	return nil
}

// forward is the single emitter for the shell session: it forwards PTY output
// as stdout frames (a terminal merges stdout/stderr, like SSH shell channels)
// and, once the shell exits, drains the remaining buffer and emits the exit
// frame — preserving frame ordering.
func (s *shellSession) forward(ctx context.Context, out <-chan []byte, emit func(frame any, final bool)) {
	exitCh := make(chan int, 1)
	go func() {
		exitCh <- s.session.WaitExit()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-out:
			if !ok {
				// Subscriber was dropped as a slow consumer; keep the
				// protocol alive — the exit frame is still delivered.
				out = nil
				continue
			}
			emit(OutputFrame{Type: FrameTypeStdout, Data: string(chunk)}, false)
		case exitCode := <-exitCh:
			// The shell process exited; flush whatever output is still
			// buffered, then terminate the protocol.
			drainTimer := time.NewTimer(exitDrainTimeout)
			defer drainTimer.Stop()
		drain:
			for {
				select {
				case chunk, ok := <-out:
					if !ok {
						out = nil
						continue
					}
					emit(OutputFrame{Type: FrameTypeStdout, Data: string(chunk)}, false)
				case <-drainTimer.C:
					break drain
				case <-ctx.Done():
					break drain
				}
			}
			emit(ExitFrame{Type: FrameTypeExit, ExitCode: exitCode}, true)
			return
		}
	}
}

func (s *shellSession) WriteStdin(data []byte) error {
	return s.session.WriteInput(data)
}

// CloseStdin delivers stdin EOF as ^D (EOT), like a terminal would.
func (s *shellSession) CloseStdin() error {
	return s.session.WriteInput([]byte{0x04})
}

func (s *shellSession) Signal(sig syscall.Signal) error {
	if ch, ok := shellSignalControlChars[sig]; ok {
		return s.session.WriteInput([]byte{ch})
	}

	// Other signals go to the shell's process group — the PTY child is a
	// session leader (Setsid), so its PGID equals its PID.
	if pid := s.session.ShellPid(); pid > 0 {
		return syscall.Kill(-pid, sig)
	}
	return nil
}

func (s *shellSession) Resize(cols, rows uint16) error {
	return s.session.Resize(cols, rows)
}

func (s *shellSession) Kill() {
	if s.session != nil {
		s.session.Kill()
	}
}
