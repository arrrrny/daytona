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

// HandleMCPPost godoc
//
//	@Summary		MCP endpoint — send JSON-RPC messages (streamable HTTP)
//	@Description	Model Context Protocol endpoint (streamable-HTTP transport) exposing sandbox tools: exec_command, fs_read_file, fs_write_file, fs_list_files. The request body is a JSON-RPC 2.0 message (initialize, tools/list, tools/call, ...); the response is a JSON-RPC response or an SSE event stream per the transport. The handler is stateless: plain HTTP clients can call tools without the initialize handshake. Authenticate with a scoped SSH access token (Authorization: Bearer <token>) exactly like /process/exec/connect. NOTE: MCP clients should speak JSON-RPC directly — generated REST clients cannot express the MCP transport.
//	@Tags			mcp
//	@Accept			json
//	@Produce		json,text/event-stream
//	@Param			message	body	object	true	"JSON-RPC 2.0 request message (e.g. tools/call)"
//	@Success		200		"JSON-RPC response or SSE event stream"
//	@Router			/mcp [post]
//
//	@id				MCPPost
func (m *MCPServer) HandleMCPPost(c *gin.Context) {
	m.handler.ServeHTTP(c.Writer, c.Request)
}

// HandleMCPGet godoc
//
//	@Summary		MCP endpoint — open the SSE stream (streamable HTTP)
//	@Description	Opens the server-sent-event stream of the MCP streamable-HTTP transport. Stateless deployments do not emit unsolicited events, so most clients only need POST.
//	@Tags			mcp
//	@Produce		text/event-stream
//	@Success		200	"SSE event stream"
//	@Router			/mcp [get]
//
//	@id				MCPGet
func (m *MCPServer) HandleMCPGet(c *gin.Context) {
	m.handler.ServeHTTP(c.Writer, c.Request)
}

// HandleMCPDelete godoc
//
//	@Summary		MCP endpoint — terminate the session (streamable HTTP)
//	@Description	Terminates the MCP session per the streamable-HTTP transport. The handler is stateless, so this is a no-op acknowledged for transport compliance.
//	@Tags			mcp
//	@Success		202	"Session terminated"
//	@Router			/mcp [delete]
//
//	@id				MCPDelete
func (m *MCPServer) HandleMCPDelete(c *gin.Context) {
	m.handler.ServeHTTP(c.Writer, c.Request)
}
