// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package mcp

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/daytonaio/daemon/internal/util"
	session_svc "github.com/daytonaio/daemon/pkg/session"
	execws "github.com/daytonaio/daemon/pkg/toolbox/process/exec"
	"github.com/google/uuid"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	defaultExecTimeoutSec = 120
	maxExecTimeoutSec     = 3600
	maxReadFileBytes      = 10 * 1024 * 1024
	execPollInterval      = 50 * time.Millisecond
)

// --- exec_command ---

type execCommandArgs struct {
	Command string            `json:"command" jsonschema:"The shell command to execute"`
	Cwd     string            `json:"cwd,omitempty" jsonschema:"Working directory for the command (defaults to the sandbox work dir)"`
	Env     map[string]string `json:"env,omitempty" jsonschema:"Additional environment variables for the command"`
	Timeout int               `json:"timeout,omitempty" jsonschema:"Timeout in seconds (default 120, max 3600)"`
}

type execCommandResult struct {
	Stdout   string `json:"stdout" jsonschema:"Standard output of the command"`
	Stderr   string `json:"stderr" jsonschema:"Standard error of the command"`
	ExitCode int    `json:"exitCode" jsonschema:"Process exit code (128+signal when killed by a signal)"`
}

func (m *MCPServer) execCommand(ctx context.Context, _ *mcpsdk.CallToolRequest, args execCommandArgs) (*mcpsdk.CallToolResult, execCommandResult, error) {
	if strings.TrimSpace(args.Command) == "" {
		return toolError("command is required"), execCommandResult{}, nil
	}

	timeout := args.Timeout
	if timeout <= 0 {
		timeout = defaultExecTimeoutSec
	}
	if timeout > maxExecTimeoutSec {
		timeout = maxExecTimeoutSec
	}

	sessionId := "mcp-" + uuid.NewString()
	if err := m.sessionService.Create(sessionId, false); err != nil {
		return nil, execCommandResult{}, fmt.Errorf("failed to create session: %w", err)
	}
	defer func() {
		deleteCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := m.sessionService.Delete(deleteCtx, sessionId); err != nil {
			m.logger.Debug("failed to delete mcp exec session", "sessionId", sessionId, "error", err)
		}
	}()

	// Reuse the session command wrapper so output demux and exit-code
	// handling behave exactly like the REST/session endpoints.
	script := execws.BuildCommandScript(args.Cwd, args.Env, args.Command, m.workDir)

	result, err := m.sessionService.Execute(sessionId, util.EmptyCommandID, script, true, false, true, true)
	if err != nil {
		return nil, execCommandResult{}, fmt.Errorf("failed to execute command: %w", err)
	}

	logPath, exitCodePath, err := m.sessionService.CommandLogPaths(sessionId, result.CommandId)
	if err != nil {
		return nil, execCommandResult{}, err
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil, execCommandResult{}, ctx.Err()
		default:
		}

		if exitCode, ok := readExitCode(exitCodePath); ok {
			stdout, stderr := demuxLogFile(logPath)
			out := execCommandResult{Stdout: stdout, Stderr: stderr, ExitCode: exitCode}
			return &mcpsdk.CallToolResult{
				Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: execResultText(out)}},
			}, out, nil
		}

		if time.Now().After(deadline) {
			_ = m.sessionService.SignalDescendants(sessionId, syscall.SIGKILL)
			// Give the wrapper a moment to flush output to the log.
			time.Sleep(200 * time.Millisecond)
			stdout, stderr := demuxLogFile(logPath)
			out := execCommandResult{Stdout: stdout, Stderr: stderr, ExitCode: -1}
			return &mcpsdk.CallToolResult{
				IsError: true,
				Content: []mcpsdk.Content{&mcpsdk.TextContent{
					Text: fmt.Sprintf("command timed out after %ds\n%s", timeout, execResultText(out)),
				}},
			}, out, nil
		}

		time.Sleep(execPollInterval)
	}
}

func execResultText(out execCommandResult) string {
	var b strings.Builder
	b.WriteString(out.Stdout)
	if out.Stderr != "" {
		if b.Len() > 0 && !strings.HasSuffix(out.Stdout, "\n") {
			b.WriteString("\n")
		}
		b.WriteString(out.Stderr)
	}
	fmt.Fprintf(&b, "\nexitCode: %d\n", out.ExitCode)
	return b.String()
}

func readExitCode(exitCodePath string) (int, bool) {
	exitCodeBytes, err := os.ReadFile(exitCodePath)
	if err != nil {
		return 0, false
	}
	var exitCode int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(exitCodeBytes)), "%d", &exitCode); err != nil {
		return 0, false
	}
	return exitCode, true
}

func demuxLogFile(logPath string) (stdout, stderr string) {
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		return "", ""
	}
	stdoutBytes, stderrBytes := session_svc.DemuxLogBytes(logBytes)
	return string(stdoutBytes), string(stderrBytes)
}

// --- fs_read_file ---

type readFileArgs struct {
	Path string `json:"path" jsonschema:"Absolute path of the file to read"`
}

type readFileResult struct {
	Path     string `json:"path" jsonschema:"The path that was read"`
	Content  string `json:"content" jsonschema:"File content (UTF-8 text, or base64 when encoding is base64)"`
	Encoding string `json:"encoding,omitempty" jsonschema:"Set to base64 for binary files"`
	Size     int64  `json:"size" jsonschema:"File size in bytes"`
}

func (m *MCPServer) readFile(_ context.Context, _ *mcpsdk.CallToolRequest, args readFileArgs) (*mcpsdk.CallToolResult, readFileResult, error) {
	if args.Path == "" {
		return toolError("path is required"), readFileResult{}, nil
	}

	// Open first and validate the opened descriptor: a pre-read os.Stat can be
	// bypassed by file growth, and special files (e.g. /dev/zero, size 0)
	// would otherwise make the read allocate without bound.
	file, err := os.Open(args.Path)
	if err != nil {
		return toolError(fmt.Sprintf("failed to open file: %v", err)), readFileResult{}, nil
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return toolError(fmt.Sprintf("failed to stat file: %v", err)), readFileResult{}, nil
	}
	if info.IsDir() {
		return toolError("path is a directory, use fs_list_files instead"), readFileResult{}, nil
	}
	if !info.Mode().IsRegular() {
		return toolError("path is not a regular file"), readFileResult{}, nil
	}
	if info.Size() > maxReadFileBytes {
		return toolError(fmt.Sprintf("file too large (%d bytes, max %d)", info.Size(), maxReadFileBytes)), readFileResult{}, nil
	}

	content, err := io.ReadAll(io.LimitReader(file, maxReadFileBytes+1))
	if err != nil {
		return toolError(fmt.Sprintf("failed to read file: %v", err)), readFileResult{}, nil
	}
	if len(content) > maxReadFileBytes {
		return toolError(fmt.Sprintf("file too large (max %d bytes)", maxReadFileBytes)), readFileResult{}, nil
	}

	out := readFileResult{Path: args.Path, Size: int64(len(content))}
	if utf8.Valid(content) {
		out.Content = string(content)
	} else {
		out.Content = base64.StdEncoding.EncodeToString(content)
		out.Encoding = "base64"
	}

	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: out.Content}},
	}, out, nil
}

// --- fs_write_file ---

type writeFileArgs struct {
	Path    string `json:"path" jsonschema:"Absolute path of the file to write"`
	Content string `json:"content" jsonschema:"Text content to write to the file"`
}

type writeFileResult struct {
	Path         string `json:"path" jsonschema:"The path that was written"`
	BytesWritten int    `json:"bytesWritten" jsonschema:"Number of bytes written"`
}

func (m *MCPServer) writeFile(_ context.Context, _ *mcpsdk.CallToolRequest, args writeFileArgs) (*mcpsdk.CallToolResult, writeFileResult, error) {
	if args.Path == "" {
		return toolError("path is required"), writeFileResult{}, nil
	}

	if dir := filepath.Dir(args.Path); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return toolError(fmt.Sprintf("failed to create parent directories: %v", err)), writeFileResult{}, nil
		}
	}

	if err := os.WriteFile(args.Path, []byte(args.Content), 0644); err != nil {
		return toolError(fmt.Sprintf("failed to write file: %v", err)), writeFileResult{}, nil
	}

	out := writeFileResult{Path: args.Path, BytesWritten: len(args.Content)}
	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{&mcpsdk.TextContent{
			Text: fmt.Sprintf("wrote %d bytes to %s", out.BytesWritten, out.Path),
		}},
	}, out, nil
}

// --- fs_list_files ---

type listFilesArgs struct {
	Path string `json:"path,omitempty" jsonschema:"Directory path to list (defaults to the current directory)"`
}

type fileEntry struct {
	Name       string    `json:"name" jsonschema:"File or directory name"`
	Size       int64     `json:"size" jsonschema:"Size in bytes"`
	IsDir      bool      `json:"isDir" jsonschema:"Whether this entry is a directory"`
	Mode       string    `json:"mode" jsonschema:"File mode string"`
	ModifiedAt time.Time `json:"modifiedAt" jsonschema:"Last modification time"`
}

type listFilesResult struct {
	Path  string      `json:"path" jsonschema:"The directory that was listed"`
	Files []fileEntry `json:"files" jsonschema:"Directory entries"`
}

func (m *MCPServer) listFiles(_ context.Context, _ *mcpsdk.CallToolRequest, args listFilesArgs) (*mcpsdk.CallToolResult, listFilesResult, error) {
	path := args.Path
	if path == "" {
		path = "."
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return toolError(fmt.Sprintf("failed to list files: %v", err)), listFilesResult{}, nil
	}

	files := make([]fileEntry, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, fileEntry{
			Name:       entry.Name(),
			Size:       info.Size(),
			IsDir:      entry.IsDir(),
			Mode:       info.Mode().String(),
			ModifiedAt: info.ModTime(),
		})
	}

	out := listFilesResult{Path: path, Files: files}
	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: listFilesText(out)}},
	}, out, nil
}

func listFilesText(out listFilesResult) string {
	var b strings.Builder
	for _, f := range out.Files {
		typeChar := "-"
		if f.IsDir {
			typeChar = "d"
		}
		fmt.Fprintf(&b, "%s %10d %s %s\n", typeChar, f.Size, f.ModifiedAt.Format(time.RFC3339), f.Name)
	}
	return b.String()
}

func toolError(message string) *mcpsdk.CallToolResult {
	return &mcpsdk.CallToolResult{
		IsError: true,
		Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: message}},
	}
}
