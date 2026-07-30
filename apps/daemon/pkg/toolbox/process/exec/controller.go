// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package exec

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"syscall"
	"time"

	"github.com/daytonaio/daemon/internal/util"
	session_svc "github.com/daytonaio/daemon/pkg/session"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	// startFrameTimeout bounds how long a client may take to send the initial
	// start frame after the WebSocket upgrade completes.
	startFrameTimeout = 30 * time.Second

	maxFrameCols = 1000
	maxFrameRows = 1000
)

// execSession abstracts a single exec connection regardless of mode:
// one-shot command (session-backed) or interactive login shell (PTY-backed).
type execSession interface {
	// Start launches the command/shell and spawns the goroutines that emit
	// stdout/stderr/exit frames via emit(frame, final). final=true marks the
	// exit/error frame after which the connection is closed.
	Start(ctx context.Context, start StartFrame, emit func(frame any, final bool)) error
	// WriteStdin delivers raw stdin bytes to the command/shell.
	WriteStdin(data []byte) error
	// CloseStdin delivers stdin EOF (SSH channel EOF semantics).
	CloseStdin() error
	// Signal delivers a signal to the foreground command/shell.
	Signal(sig syscall.Signal) error
	// Resize changes the terminal window size (no-op without a PTY).
	Resize(cols, rows uint16) error
	// Kill tears down the session and every process it spawned.
	Kill()
}

type ExecController struct {
	logger         *slog.Logger
	workDir        string
	sessionService *session_svc.SessionService
}

func NewExecController(logger *slog.Logger, workDir string, sessionService *session_svc.SessionService) *ExecController {
	return &ExecController{
		logger:         logger.With(slog.String("component", "exec_controller")),
		workDir:        workDir,
		sessionService: sessionService,
	}
}

// Connect godoc
//
//	@Summary		Execute a command or open a shell over a single WebSocket connection
//	@Description	SSH-equivalent exec channel over HTTPS. After the upgrade the client sends a start frame: {"type":"start","command":"...","cwd":"...","env":{...},"cols":...,"rows":...}. When command is omitted, an interactive login shell is started (like bare `ssh host`). Subsequent client frames: stdin, signal, resize, stdin_eof. Server frames: stdout, stderr, exit (always last, before close), error. One connection = one exec; shell state persists for the lifetime of the connection.
//	@Tags			process
//	@Param			token	query	string	false	"SSH access token (alternative to the Authorization header for WS clients that cannot set headers)"
//	@Success		101		"Switching Protocols - WebSocket connection established"
//	@Router			/process/exec/connect [get]
//
//	@id				ExecConnect
func (e *ExecController) Connect(c *gin.Context) {
	ws, err := util.UpgradeToWebSocket(c.Writer, c.Request)
	if err != nil {
		e.logger.Error("ws upgrade failed", "error", err)
		return
	}

	e.handleConnection(ws)
}

// outboundFrame is a unit of work for the single writer goroutine; close=true
// closes the WebSocket after the frame has been written.
type outboundFrame struct {
	payload any
	close   bool
}

func (e *ExecController) handleConnection(ws *websocket.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := e.logger

	frames := make(chan outboundFrame, 64)
	pongCh := util.SetupWSKeepAlive(ws, logger)

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			select {
			case frame := <-frames:
				util.WritePendingPongs(ws, pongCh, time.Second, logger)

				data, err := json.Marshal(frame.payload)
				if err != nil {
					logger.Error("failed to marshal exec frame", "error", err)
					continue
				}
				_ = ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
					logger.Debug("exec ws write error", "error", err)
					return
				}
				if frame.close {
					_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	emit := func(payload any, final bool) {
		select {
		case frames <- outboundFrame{payload: payload, close: final}:
		case <-ctx.Done():
		}
	}

	var sess execSession

	defer func() {
		// Cancel first: emit() selects on ctx.Done, so session goroutines
		// stop enqueueing frames; the writer goroutine exits on ctx.Done.
		// The frames channel is deliberately never closed — that would race
		// with in-flight emitters and cause a send-on-closed-channel panic.
		cancel()
		if sess != nil {
			sess.Kill()
		}
		<-writerDone
		_ = ws.Close()
	}()

	fail := func(err error) {
		emit(ErrorFrame{Type: FrameTypeError, Message: err.Error()}, true)
	}

	// The first client frame must be the start frame.
	_ = ws.SetReadDeadline(time.Now().Add(startFrameTimeout))
	start, err := readStartFrame(ws)
	if err != nil {
		fail(err)
		return
	}
	// No read deadline for the rest of the session — long-running commands
	// must not be killed by an idle stdin.
	_ = ws.SetReadDeadline(time.Time{})

	if start.Cols > maxFrameCols || start.Rows > maxFrameRows {
		fail(fmt.Errorf("invalid value for cols/rows - must be less than %d", maxFrameCols))
		return
	}

	if strings.TrimSpace(start.Command) == "" {
		sess = newShellSession(logger, e.workDir)
	} else {
		sess = newCommandSession(logger, e.workDir, e.sessionService)
	}

	if err := sess.Start(ctx, *start, emit); err != nil {
		logger.Error("failed to start exec session", "error", err)
		fail(fmt.Errorf("failed to start exec session: %w", err))
		return
	}

	// Read loop: dispatch client control frames until disconnect.
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				logger.Debug("exec ws read error", "error", err)
			}
			return
		}

		var frame ClientFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			emit(ErrorFrame{Type: FrameTypeError, Message: fmt.Sprintf("invalid frame: %v", err)}, false)
			continue
		}

		switch frame.Type {
		case FrameTypeStdin:
			if err := sess.WriteStdin([]byte(frame.Data)); err != nil {
				logger.Debug("stdin write failed", "error", err)
				emit(ErrorFrame{Type: FrameTypeError, Message: fmt.Sprintf("stdin write failed: %v", err)}, false)
			}
		case FrameTypeStdinEOF:
			if err := sess.CloseStdin(); err != nil {
				logger.Debug("stdin close failed", "error", err)
				emit(ErrorFrame{Type: FrameTypeError, Message: fmt.Sprintf("stdin close failed: %v", err)}, false)
			}
		case FrameTypeSignal:
			sig, ok := parseSignal(frame.Signal)
			if !ok {
				emit(ErrorFrame{Type: FrameTypeError, Message: fmt.Sprintf("unknown signal: %q", frame.Signal)}, false)
				continue
			}
			if err := sess.Signal(sig); err != nil {
				logger.Debug("signal failed", "signal", frame.Signal, "error", err)
				emit(ErrorFrame{Type: FrameTypeError, Message: fmt.Sprintf("signal failed: %v", err)}, false)
			}
		case FrameTypeResize:
			if frame.Cols > maxFrameCols || frame.Rows > maxFrameRows {
				emit(ErrorFrame{Type: FrameTypeError, Message: fmt.Sprintf("invalid value for cols/rows - must be less than %d", maxFrameCols)}, false)
				continue
			}
			if frame.Cols == 0 || frame.Rows == 0 {
				continue
			}
			if err := sess.Resize(frame.Cols, frame.Rows); err != nil {
				logger.Debug("resize failed", "error", err)
				emit(ErrorFrame{Type: FrameTypeError, Message: fmt.Sprintf("resize failed: %v", err)}, false)
			}
		case FrameTypeStart:
			emit(ErrorFrame{Type: FrameTypeError, Message: "session already started"}, false)
		default:
			emit(ErrorFrame{Type: FrameTypeError, Message: fmt.Sprintf("unknown frame type: %q", frame.Type)}, false)
		}
	}
}

func readStartFrame(ws *websocket.Conn) (*StartFrame, error) {
	_, data, err := ws.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("failed to read start frame: %w", err)
	}

	var frame StartFrame
	if err := json.Unmarshal(data, &frame); err != nil {
		return nil, fmt.Errorf("invalid start frame: %w", err)
	}
	if frame.Type != FrameTypeStart {
		return nil, fmt.Errorf("first frame must be of type %q, got %q", FrameTypeStart, frame.Type)
	}
	if frame.Env == nil {
		frame.Env = map[string]string{}
	}

	return &frame, nil
}

// parseSignal maps SSH-style signal names to syscall signals.
func parseSignal(name string) (syscall.Signal, bool) {
	sig, ok := signalNames[strings.ToUpper(name)]
	return sig, ok
}

var signalNames = map[string]syscall.Signal{
	"SIGHUP":  syscall.SIGHUP,
	"SIGINT":  syscall.SIGINT,
	"SIGQUIT": syscall.SIGQUIT,
	"SIGKILL": syscall.SIGKILL,
	"SIGALRM": syscall.SIGALRM,
	"SIGTERM": syscall.SIGTERM,
	"SIGUSR1": syscall.SIGUSR1,
	"SIGUSR2": syscall.SIGUSR2,
	"SIGPIPE": syscall.SIGPIPE,
	"SIGSTOP": syscall.SIGSTOP,
	"SIGTSTP": syscall.SIGTSTP,
	"SIGCONT": syscall.SIGCONT,
	// Bare names, mirroring `kill -s NAME` convenience
	"HUP":  syscall.SIGHUP,
	"INT":  syscall.SIGINT,
	"QUIT": syscall.SIGQUIT,
	"KILL": syscall.SIGKILL,
	"ALRM": syscall.SIGALRM,
	"TERM": syscall.SIGTERM,
	"USR1": syscall.SIGUSR1,
	"USR2": syscall.SIGUSR2,
	"PIPE": syscall.SIGPIPE,
	"STOP": syscall.SIGSTOP,
	"TSTP": syscall.SIGTSTP,
	"CONT": syscall.SIGCONT,
}
