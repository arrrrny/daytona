// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package exec

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/daytonaio/daemon/internal/util"
	session_svc "github.com/daytonaio/daemon/pkg/session"
	"github.com/google/uuid"
)

const (
	// execPollInterval mirrors the exit-code poll cadence of SessionService.Execute.
	execPollInterval = 50 * time.Millisecond
)

// commandSession runs a one-shot command on top of a dedicated SessionService
// session, reusing the existing command wrapper (cmdWrapperFormat): the
// wrapper's stdout/stderr labelers feed a per-command log file that this
// session tails and demultiplexes into stdout/stderr frames, and its
// exit-code file delivers the SSH-style exit status.
type commandSession struct {
	logger         *slog.Logger
	workDir        string
	sessionService *session_svc.SessionService

	sessionId string
	commandId string
	killOnce  sync.Once
}

func newCommandSession(logger *slog.Logger, workDir string, sessionService *session_svc.SessionService) *commandSession {
	return &commandSession{
		logger:         logger,
		workDir:        workDir,
		sessionService: sessionService,
	}
}

func (s *commandSession) Start(ctx context.Context, start StartFrame, emit func(frame any, final bool)) error {
	s.sessionId = "exec-" + uuid.NewString()

	if err := s.sessionService.Create(s.sessionId, false); err != nil {
		return fmt.Errorf("failed to create exec session: %w", err)
	}

	script := BuildCommandScript(start.Cwd, start.Env, start.Command, s.workDir)

	// Async execute: the command wrapper keeps stdin open on a FIFO (stdin
	// frames), prefixes stdout/stderr into the command log (output frames)
	// and finally records the exit code (exit frame).
	result, err := s.sessionService.Execute(s.sessionId, util.EmptyCommandID, script, true, false, true, true)
	if err != nil {
		s.Kill()
		return fmt.Errorf("failed to execute command: %w", err)
	}
	s.commandId = result.CommandId

	logPath, exitCodePath, err := s.sessionService.CommandLogPaths(s.sessionId, s.commandId)
	if err != nil {
		s.Kill()
		return fmt.Errorf("failed to resolve command log paths: %w", err)
	}

	go s.pump(ctx, logPath, exitCodePath, emit)
	return nil
}

// pump tails the command log, demultiplexes the wrapper's stdout/stderr
// stream markers into frames, and emits the exit frame once the wrapper
// records the exit code. The wrapper guarantees the exit-code file is written
// only after all output has been flushed to the log, so once it appears the
// remaining log content can be drained to EOF before exiting.
func (s *commandSession) pump(ctx context.Context, logPath, exitCodePath string, emit func(frame any, final bool)) {
	var logFile *os.File
	defer func() {
		if logFile != nil {
			_ = logFile.Close()
		}
	}()

	var offset int64
	buf := make([]byte, 32*1024)
	demux := newStreamDemux(func(kind streamKind, data []byte) {
		frameType := FrameTypeStdout
		if kind == streamStderr {
			frameType = FrameTypeStderr
		}
		emit(OutputFrame{Type: frameType, Data: string(data)}, false)
	})

	readAvailable := func() {
		if logFile == nil {
			f, err := os.Open(logPath)
			if err != nil {
				return // not created yet
			}
			logFile = f
		}
		for {
			n, err := logFile.ReadAt(buf, offset)
			if n > 0 {
				offset += int64(n)
				demux.Write(buf[:n])
			}
			if err != nil {
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		readAvailable()

		exitCode, ok := readExitCodeFile(exitCodePath)
		if ok {
			// The wrapper flushed all output before writing the exit code;
			// drain whatever is left and terminate the protocol.
			readAvailable()
			demux.Flush()
			emit(ExitFrame{Type: FrameTypeExit, ExitCode: exitCode}, true)
			return
		}

		time.Sleep(execPollInterval)
	}
}

func (s *commandSession) WriteStdin(data []byte) error {
	return s.sessionService.WriteInput(s.sessionId, s.commandId, data)
}

func (s *commandSession) CloseStdin() error {
	return s.sessionService.CloseInput(s.sessionId, s.commandId)
}

func (s *commandSession) Signal(sig syscall.Signal) error {
	return s.sessionService.SignalDescendants(s.sessionId, sig)
}

// Resize is a no-op for session-backed exec (no PTY); the frame is accepted
// to keep the protocol uniform across modes.
func (s *commandSession) Resize(_, _ uint16) error {
	return nil
}

func (s *commandSession) Kill() {
	s.killOnce.Do(func() {
		if s.sessionId == "" {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := s.sessionService.Delete(ctx, s.sessionId); err != nil {
			s.logger.Debug("failed to delete exec session", "sessionId", s.sessionId, "error", err)
		}
	})
}

func readExitCodeFile(exitCodePath string) (int, bool) {
	exitCodeBytes, err := os.ReadFile(exitCodePath)
	if err != nil {
		return 0, false
	}
	exitCode, err := strconv.Atoi(strings.TrimRight(string(exitCodeBytes), "\n"))
	if err != nil {
		return 0, false
	}
	return exitCode, true
}

var envKeyRegex = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// BuildCommandScript renders a command script for the session command wrapper
// that applies cwd and env before running the command itself.
//
// The script is wrapped in a subshell on purpose: the session command
// wrapper sources the script in the session shell ({ . cmdfile; }), so a
// bare `exit` in the command — or a failing `cd` — would kill the session
// shell before the wrapper could record the exit code. The subshell confines
// any `exit` and surfaces its status as the command's exit code instead.
func BuildCommandScript(cwd string, env map[string]string, command, fallbackCwd string) string {
	var b strings.Builder
	b.WriteString("(\n")

	dir := cwd
	if dir == "" {
		dir = fallbackCwd
	}
	if dir != "" {
		b.WriteString("cd " + shellQuote(dir) + " || exit 1\n")
	}

	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !envKeyRegex.MatchString(k) {
			continue
		}
		b.WriteString("export " + k + "=" + shellQuote(env[k]) + "\n")
	}

	b.WriteString(command)
	if !strings.HasSuffix(command, "\n") {
		b.WriteString("\n")
	}
	b.WriteString(")\n")

	return b.String()
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
