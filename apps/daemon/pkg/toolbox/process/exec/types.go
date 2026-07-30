// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package exec

// Frame protocol for GET /process/exec/connect — an SSH-equivalent exec
// channel over a single WebSocket connection.
//
// Client -> server frames:
//
//	{"type":"start","command":"echo hi","cwd":"/home/daytona","env":{"FOO":"bar"},"cols":120,"rows":40}
//	{"type":"stdin","data":"..."}
//	{"type":"signal","signal":"SIGINT"}
//	{"type":"resize","cols":120,"rows":40}
//	{"type":"stdin_eof"}
//
// Server -> client frames:
//
//	{"type":"stdout","data":"..."}
//	{"type":"stderr","data":"..."}
//	{"type":"exit","exitCode":0}
//	{"type":"error","message":"..."}
//
// The first client frame must be "start". When "command" is omitted, an
// interactive login shell is started instead of a one-shot command — exactly
// like bare `ssh host`. The "exit" frame is always the last server frame and
// is delivered before the connection is closed (SSH exit-status semantics).
const (
	FrameTypeStart    = "start"
	FrameTypeStdin    = "stdin"
	FrameTypeSignal   = "signal"
	FrameTypeResize   = "resize"
	FrameTypeStdinEOF = "stdin_eof"

	FrameTypeStdout = "stdout"
	FrameTypeStderr = "stderr"
	FrameTypeExit   = "exit"
	FrameTypeError  = "error"
)

// StartFrame is the first (and only) configuration frame sent by the client.
type StartFrame struct {
	Type    string            `json:"type"`
	Command string            `json:"command,omitempty"`
	Cwd     string            `json:"cwd,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Cols    uint16            `json:"cols,omitempty"`
	Rows    uint16            `json:"rows,omitempty"`
} //	@name	ExecStartFrame

// ClientFrame is any control frame sent by the client after "start".
type ClientFrame struct {
	Type   string `json:"type"`
	Data   string `json:"data,omitempty"`
	Signal string `json:"signal,omitempty"`
	Cols   uint16 `json:"cols,omitempty"`
	Rows   uint16 `json:"rows,omitempty"`
} //	@name	ExecClientFrame

// OutputFrame carries stdout or stderr data from the command/shell.
type OutputFrame struct {
	Type string `json:"type"`
	Data string `json:"data"`
} //	@name	ExecOutputFrame

// ExitFrame terminates the protocol; exitCode uses SSH exit-status semantics
// (128+signal when the command was killed by a signal, e.g. 130 for SIGINT).
type ExitFrame struct {
	Type     string `json:"type"`
	ExitCode int    `json:"exitCode"`
} //	@name	ExecExitFrame

// ErrorFrame reports a fatal protocol error; the connection closes after it.
type ErrorFrame struct {
	Type    string `json:"type"`
	Message string `json:"message"`
} //	@name	ExecErrorFrame
