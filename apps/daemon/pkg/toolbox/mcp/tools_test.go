// Copyright Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package mcp

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	session_svc "github.com/daytonaio/daemon/pkg/session"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func newTestMCPServer(t *testing.T) *MCPServer {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sessionService := session_svc.NewSessionService(logger, t.TempDir(), 250*time.Millisecond, 25*time.Millisecond)
	return NewMCPServer(logger, t.TempDir(), sessionService)
}

func textOf(t *testing.T, result *mcpsdk.CallToolResult) string {
	t.Helper()
	var b strings.Builder
	for _, content := range result.Content {
		if tc, ok := content.(*mcpsdk.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

func TestExecCommandTool(t *testing.T) {
	m := newTestMCPServer(t)

	result, out, err := m.execCommand(context.Background(), nil, execCommandArgs{Command: "echo hi"})
	if err != nil {
		t.Fatalf("execCommand failed: %v", err)
	}
	if result.IsError {
		t.Fatalf("expected success, got error result: %s", textOf(t, result))
	}
	if out.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", out.ExitCode)
	}
	if !strings.Contains(out.Stdout, "hi") {
		t.Fatalf("expected stdout to contain 'hi', got %q", out.Stdout)
	}
	if !strings.Contains(textOf(t, result), "hi") {
		t.Fatalf("expected MCP content to contain 'hi', got %q", textOf(t, result))
	}
}

func TestExecCommandToolStderrAndExitCode(t *testing.T) {
	m := newTestMCPServer(t)

	_, out, err := m.execCommand(context.Background(), nil, execCommandArgs{Command: "echo oops >&2; exit 42"})
	if err != nil {
		t.Fatalf("execCommand failed: %v", err)
	}
	if out.ExitCode != 42 {
		t.Fatalf("expected exit code 42, got %d", out.ExitCode)
	}
	if !strings.Contains(out.Stderr, "oops") {
		t.Fatalf("expected stderr to contain 'oops', got %q", out.Stderr)
	}
}

func TestExecCommandToolCwdAndEnv(t *testing.T) {
	m := newTestMCPServer(t)
	dir := t.TempDir()

	_, out, err := m.execCommand(context.Background(), nil, execCommandArgs{
		Command: "pwd && echo $FOO",
		Cwd:     dir,
		Env:     map[string]string{"FOO": "bar"},
	})
	if err != nil {
		t.Fatalf("execCommand failed: %v", err)
	}
	if !strings.Contains(out.Stdout, dir) {
		t.Fatalf("expected stdout to contain cwd %q, got %q", dir, out.Stdout)
	}
	if !strings.Contains(out.Stdout, "bar") {
		t.Fatalf("expected stdout to contain env value 'bar', got %q", out.Stdout)
	}
}

func TestExecCommandToolTimeout(t *testing.T) {
	m := newTestMCPServer(t)

	start := time.Now()
	result, _, err := m.execCommand(context.Background(), nil, execCommandArgs{Command: "sleep 30", Timeout: 1})
	if err != nil {
		t.Fatalf("execCommand failed: %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected timeout error result, got %s", textOf(t, result))
	}
	if !strings.Contains(textOf(t, result), "timed out") {
		t.Fatalf("expected timeout message, got %q", textOf(t, result))
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("timeout took too long: %s", elapsed)
	}
}

func TestFsWriteAndReadFileTools(t *testing.T) {
	m := newTestMCPServer(t)
	path := filepath.Join(t.TempDir(), "sub", "dir", "test.txt")

	writeResult, writeOut, err := m.writeFile(context.Background(), nil, writeFileArgs{
		Path:    path,
		Content: "hello world",
	})
	if err != nil {
		t.Fatalf("writeFile failed: %v", err)
	}
	if writeResult.IsError {
		t.Fatalf("expected success, got %s", textOf(t, writeResult))
	}
	if writeOut.BytesWritten != len("hello world") {
		t.Fatalf("expected %d bytes written, got %d", len("hello world"), writeOut.BytesWritten)
	}

	readResult, readOut, err := m.readFile(context.Background(), nil, readFileArgs{Path: path})
	if err != nil {
		t.Fatalf("readFile failed: %v", err)
	}
	if readResult.IsError {
		t.Fatalf("expected success, got %s", textOf(t, readResult))
	}
	if readOut.Content != "hello world" {
		t.Fatalf("expected 'hello world', got %q", readOut.Content)
	}
}

func TestFsReadFileToolNotFound(t *testing.T) {
	m := newTestMCPServer(t)

	result, _, err := m.readFile(context.Background(), nil, readFileArgs{Path: filepath.Join(t.TempDir(), "nope.txt")})
	if err != nil {
		t.Fatalf("readFile failed: %v", err)
	}
	if !result.IsError {
		t.Fatalf("expected error result for missing file")
	}
}

func TestFsListFilesTool(t *testing.T) {
	m := newTestMCPServer(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0755); err != nil {
		t.Fatal(err)
	}

	result, out, err := m.listFiles(context.Background(), nil, listFilesArgs{Path: dir})
	if err != nil {
		t.Fatalf("listFiles failed: %v", err)
	}
	if result.IsError {
		t.Fatalf("expected success, got %s", textOf(t, result))
	}
	if len(out.Files) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(out.Files))
	}

	var sawFile, sawDir bool
	for _, f := range out.Files {
		if f.Name == "a.txt" && !f.IsDir {
			sawFile = true
		}
		if f.Name == "subdir" && f.IsDir {
			sawDir = true
		}
	}
	if !sawFile || !sawDir {
		t.Fatalf("expected file and dir entries, got %+v", out.Files)
	}
}

// TestMCPHTTPEndpoint exercises the full streamable-HTTP transport the way
// the issue's acceptance criteria do: initialize + tools/list + tools/call
// over plain POSTs.
func TestMCPHTTPEndpoint(t *testing.T) {
	m := newTestMCPServer(t)
	httpServer := httptest.NewServer(m.handler)
	t.Cleanup(httpServer.Close)

	post := func(payload string) string {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, httpServer.URL, strings.NewReader(payload))
		if err != nil {
			t.Fatalf("build MCP request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		resp, err := httpServer.Client().Do(req)
		if err != nil {
			t.Fatalf("MCP POST failed: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read MCP response: %v", err)
		}
		return string(body)
	}

	initResp := post(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}`)
	if !strings.Contains(initResp, "daytona-toolbox") {
		t.Fatalf("expected initialize response to name the server, got %q", initResp)
	}

	listResp := post(`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	for _, tool := range []string{"exec_command", "fs_read_file", "fs_write_file", "fs_list_files"} {
		if !strings.Contains(listResp, tool) {
			t.Fatalf("expected tools/list to contain %q, got %q", tool, listResp)
		}
	}

	callResp := post(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"exec_command","arguments":{"command":"echo hi"}}}`)
	if !strings.Contains(callResp, "hi") {
		t.Fatalf("expected tools/call response to contain 'hi', got %q", callResp)
	}
}
