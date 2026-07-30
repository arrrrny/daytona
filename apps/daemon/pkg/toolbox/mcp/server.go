// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package mcp

import (
	"log/slog"
	"net/http"

	"github.com/daytonaio/daemon/internal"
	session_svc "github.com/daytonaio/daemon/pkg/session"
	"github.com/gin-gonic/gin"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// MCPServer exposes sandbox tools (command execution + filesystem) over the
// Model Context Protocol, so MCP-native agents can use a Daytona sandbox by
// pointing their MCP client at a single URL + SSH access token.
type MCPServer struct {
	logger         *slog.Logger
	workDir        string
	sessionService *session_svc.SessionService
	handler        http.Handler
}

// NewMCPServer builds the MCP endpoint: a single streamable-HTTP handler
// (POST for JSON-RPC messages, GET for the SSE stream per the MCP
// streamable-HTTP transport) serving the v1 toolset. The handler is
// stateless: every POST is self-contained, so plain HTTP clients can call
// tools without the initialize handshake.
func NewMCPServer(logger *slog.Logger, workDir string, sessionService *session_svc.SessionService) *MCPServer {
	m := &MCPServer{
		logger:         logger.With(slog.String("component", "mcp_server")),
		workDir:        workDir,
		sessionService: sessionService,
	}

	server := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "daytona-toolbox",
		Version: internal.Version,
	}, nil)

	mcpsdk.AddTool(server, &mcpsdk.Tool{
		Name:        "exec_command",
		Description: "Execute a shell command inside the sandbox and return its stdout, stderr and exit code.",
	}, m.execCommand)

	mcpsdk.AddTool(server, &mcpsdk.Tool{
		Name:        "fs_read_file",
		Description: "Read a file from the sandbox filesystem. Returns UTF-8 text, or base64 for binary files.",
	}, m.readFile)

	mcpsdk.AddTool(server, &mcpsdk.Tool{
		Name:        "fs_write_file",
		Description: "Write text content to a file on the sandbox filesystem, creating parent directories as needed.",
	}, m.writeFile)

	mcpsdk.AddTool(server, &mcpsdk.Tool{
		Name:        "fs_list_files",
		Description: "List files and directories at a path on the sandbox filesystem.",
	}, m.listFiles)

	m.handler = mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server {
		return server
	}, &mcpsdk.StreamableHTTPOptions{
		Stateless: true,
	})

	return m
}

// HandleMCP godoc
//
//	@Summary		MCP endpoint (streamable HTTP)
//	@Description	Model Context Protocol endpoint (streamable-HTTP transport) exposing sandbox tools: exec_command, fs_read_file, fs_write_file, fs_list_files. POST sends JSON-RPC messages (responses are SSE events per the transport); GET opens the SSE stream. Authenticate with a scoped SSH access token (Authorization: Bearer <token>) exactly like /process/exec/connect.
//	@Tags			mcp
//	@Accept			json
//	@Produce		json, text/event-stream
//	@Success		200
//	@Router			/mcp [post]
//
//	@id				MCP
func (m *MCPServer) HandleMCP(c *gin.Context) {
	m.handler.ServeHTTP(c.Writer, c.Request)
}
