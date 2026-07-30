// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package session

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	common_errors "github.com/daytonaio/common-go/pkg/errors"
)

// inputHolderPidFileName is written by cmdWrapperFormat next to the command's
// log file and holds the PID of the async stdin-holder process.
const inputHolderPidFileName = "input_holder.pid"

// CommandLogPaths returns the log file and exit-code file paths for a
// command, so streaming consumers (e.g. the exec WebSocket endpoint) can tail
// output and detect completion without going through the REST endpoints.
func (s *SessionService) CommandLogPaths(sessionId, commandId string) (logPath, exitCodePath string, err error) {
	session, ok := s.sessions.Get(sessionId)
	if !ok {
		return "", "", common_errors.NewNotFoundError(errors.New("session not found"))
	}

	command, ok := session.commands.Get(commandId)
	if !ok {
		return "", "", common_errors.NewNotFoundError(errors.New("command not found"))
	}

	logPath, exitCodePath = command.LogFilePath(session.Dir(s.configDir))
	return logPath, exitCodePath, nil
}

// WriteInput writes raw bytes to a running command's stdin FIFO. Unlike
// SendInput it adds no trailing newline and does not echo into the log —
// semantics required by byte-exact protocols (exec-over-WebSocket stdin
// frames). The FIFO is opened non-blocking first so a missing reader
// (command already gone) fails fast instead of hanging the caller.
func (s *SessionService) WriteInput(sessionId, commandId string, data []byte) error {
	session, ok := s.sessions.Get(sessionId)
	if !ok {
		return common_errors.NewNotFoundError(errors.New("session not found"))
	}

	if session.cmd.ProcessState != nil && session.cmd.ProcessState.Exited() {
		return common_errors.NewGoneError(errors.New("session process has exited"))
	}

	command, ok := session.commands.Get(commandId)
	if !ok {
		return common_errors.NewNotFoundError(errors.New("command not found"))
	}

	if command.ExitCode != nil {
		return common_errors.NewGoneError(fmt.Errorf("command has already completed with exit code %d", *command.ExitCode))
	}

	inputFilePath := command.InputFilePath(session.Dir(s.configDir))

	fd, err := syscall.Open(inputFilePath, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, syscall.ENXIO) || os.IsNotExist(err) {
			return common_errors.NewGoneError(errors.New("command stdin is closed"))
		}
		return common_errors.NewInternalServerError(fmt.Errorf("failed to open input pipe: %w", err))
	}
	defer func() { _ = syscall.Close(fd) }()

	// Restore blocking semantics for the write itself so large frames don't
	// fail with EAGAIN on a full pipe buffer.
	if err := syscall.SetNonblock(fd, false); err != nil {
		return common_errors.NewInternalServerError(fmt.Errorf("failed to configure input pipe: %w", err))
	}

	if _, err := syscall.Write(fd, data); err != nil {
		return common_errors.NewInternalServerError(fmt.Errorf("failed to write to input pipe: %w", err))
	}

	return nil
}

// CloseInput delivers stdin EOF to a running command by tearing down the
// input-holder process that cmdWrapperFormat keeps alive for async commands.
// Once the holder (and its current `sleep` child, which inherits the FIFO's
// write end) is gone, the command's stdin sees EOF — SSH channel EOF
// semantics. Best effort: if the holder is not up yet or already gone, the
// command's stdin stays as-is and nil is returned.
func (s *SessionService) CloseInput(sessionId, commandId string) error {
	session, ok := s.sessions.Get(sessionId)
	if !ok {
		return common_errors.NewNotFoundError(errors.New("session not found"))
	}

	command, ok := session.commands.Get(commandId)
	if !ok {
		return common_errors.NewNotFoundError(errors.New("command not found"))
	}

	if command.ExitCode != nil {
		return common_errors.NewGoneError(fmt.Errorf("command has already completed with exit code %d", *command.ExitCode))
	}

	pidFilePath := filepath.Join(session.Dir(s.configDir), commandId, inputHolderPidFileName)
	pidBytes, err := os.ReadFile(pidFilePath)
	if err != nil {
		return nil
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil || pid <= 0 {
		return nil
	}

	// Kill the holder's children first (the current `sleep 3600` inherits the
	// FIFO's write end and would keep stdin open), then the holder itself.
	_ = s.signalProcessTree(pid, syscall.SIGKILL)
	if holder, err := os.FindProcess(pid); err == nil {
		_ = holder.Signal(syscall.SIGKILL)
	}

	return nil
}

// SignalDescendants delivers sig to every descendant of the session's shell
// process — i.e. the currently running command pipeline (command subshell,
// labelers, stdin holder) — without touching the shell itself, so the wrapper
// survives to record the command's exit code (e.g. 130 for SIGINT).
func (s *SessionService) SignalDescendants(sessionId string, sig syscall.Signal) error {
	session, ok := s.sessions.Get(sessionId)
	if !ok {
		return common_errors.NewNotFoundError(errors.New("session not found"))
	}

	if session.cmd == nil || session.cmd.Process == nil {
		return common_errors.NewGoneError(errors.New("session process is not running"))
	}

	if session.cmd.ProcessState != nil && session.cmd.ProcessState.Exited() {
		return common_errors.NewGoneError(errors.New("session process has exited"))
	}

	return s.signalProcessTree(session.cmd.Process.Pid, sig)
}
