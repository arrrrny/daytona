// Copyright Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package exec

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	session_svc "github.com/daytonaio/daemon/pkg/session"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func newTestServer(t *testing.T) (*httptest.Server, *session_svc.SessionService) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sessionService := session_svc.NewSessionService(logger, t.TempDir(), 250*time.Millisecond, 25*time.Millisecond)
	controller := NewExecController(logger, t.TempDir(), sessionService)

	r := gin.New()
	r.GET("/process/exec/connect", controller.Connect)

	server := httptest.NewServer(r)
	t.Cleanup(server.Close)

	return server, sessionService
}

func dialExec(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()

	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/process/exec/connect"
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("failed to dial exec endpoint: %v", err)
	}
	t.Cleanup(func() { _ = ws.Close() })

	return ws
}

type serverFrame struct {
	Type     string `json:"type"`
	Data     string `json:"data"`
	ExitCode int    `json:"exitCode"`
	Message  string `json:"message"`
}

// collectFrames reads frames until the exit frame (or timeout) and returns
// all frames received.
func collectFrames(t *testing.T, ws *websocket.Conn, timeout time.Duration) []serverFrame {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var frames []serverFrame

	for time.Now().Before(deadline) {
		_ = ws.SetReadDeadline(deadline)
		_, data, err := ws.ReadMessage()
		if err != nil {
			break
		}

		var frame serverFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			t.Fatalf("invalid server frame: %v (%s)", err, string(data))
		}
		frames = append(frames, frame)

		if frame.Type == FrameTypeExit {
			return frames
		}
	}

	t.Fatalf("did not receive exit frame within %s; frames: %+v", timeout, frames)
	return nil
}

func stdoutOf(frames []serverFrame) string {
	var b strings.Builder
	for _, f := range frames {
		if f.Type == FrameTypeStdout {
			b.WriteString(f.Data)
		}
	}
	return b.String()
}

func stderrOf(frames []serverFrame) string {
	var b strings.Builder
	for _, f := range frames {
		if f.Type == FrameTypeStderr {
			b.WriteString(f.Data)
		}
	}
	return b.String()
}

func exitCodeOf(t *testing.T, frames []serverFrame) int {
	t.Helper()
	for i := len(frames) - 1; i >= 0; i-- {
		if frames[i].Type == FrameTypeExit {
			return frames[i].ExitCode
		}
	}
	t.Fatal("no exit frame received")
	return -1
}

func sendFrame(t *testing.T, ws *websocket.Conn, frame any) {
	t.Helper()
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func TestExecCommandStreamsStdoutAndExitCode(t *testing.T) {
	server, _ := newTestServer(t)
	ws := dialExec(t, server)

	sendFrame(t, ws, StartFrame{Type: FrameTypeStart, Command: "echo hello && pwd", Cwd: "/tmp"})

	frames := collectFrames(t, ws, 10*time.Second)

	if out := stdoutOf(frames); !strings.Contains(out, "hello") || !strings.Contains(out, "/tmp") {
		t.Fatalf("expected stdout to contain 'hello' and '/tmp', got %q", out)
	}
	if code := exitCodeOf(t, frames); code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}
}

func TestExecCommandSeparatesStderr(t *testing.T) {
	server, _ := newTestServer(t)
	ws := dialExec(t, server)

	sendFrame(t, ws, StartFrame{Type: FrameTypeStart, Command: "echo out; echo err >&2; exit 3"})

	frames := collectFrames(t, ws, 10*time.Second)

	if out := stdoutOf(frames); !strings.Contains(out, "out") {
		t.Fatalf("expected stdout to contain 'out', got %q", out)
	}
	if errOut := stderrOf(frames); !strings.Contains(errOut, "err") {
		t.Fatalf("expected stderr to contain 'err', got %q", errOut)
	}
	if code := exitCodeOf(t, frames); code != 3 {
		t.Fatalf("expected exit code 3, got %d", code)
	}
}

func TestExecCommandStdinAndStdinEOF(t *testing.T) {
	server, _ := newTestServer(t)
	ws := dialExec(t, server)

	sendFrame(t, ws, StartFrame{Type: FrameTypeStart, Command: "cat"})

	// Give the command a moment to come up before writing stdin.
	time.Sleep(300 * time.Millisecond)
	sendFrame(t, ws, ClientFrame{Type: FrameTypeStdin, Data: "ping\n"})
	time.Sleep(300 * time.Millisecond)
	sendFrame(t, ws, ClientFrame{Type: FrameTypeStdinEOF})

	frames := collectFrames(t, ws, 10*time.Second)

	if out := stdoutOf(frames); !strings.Contains(out, "ping") {
		t.Fatalf("expected stdout to contain echoed stdin 'ping', got %q", out)
	}
	if code := exitCodeOf(t, frames); code != 0 {
		t.Fatalf("expected exit code 0 after stdin_eof, got %d", code)
	}
}

func TestExecCommandSigintDeliversExitCode130(t *testing.T) {
	server, _ := newTestServer(t)
	ws := dialExec(t, server)

	sendFrame(t, ws, StartFrame{Type: FrameTypeStart, Command: "sleep 60"})

	// Let sleep start before signaling.
	time.Sleep(500 * time.Millisecond)
	sendFrame(t, ws, ClientFrame{Type: FrameTypeSignal, Signal: "SIGINT"})

	frames := collectFrames(t, ws, 10*time.Second)

	if code := exitCodeOf(t, frames); code != 130 {
		t.Fatalf("expected exit code 130 after SIGINT, got %d (frames: %+v)", code, frames)
	}
}

func TestExecShellModePersistsShellState(t *testing.T) {
	server, _ := newTestServer(t)
	ws := dialExec(t, server)

	sendFrame(t, ws, StartFrame{Type: FrameTypeStart})

	// Interactive login shell takes a moment to initialize.
	time.Sleep(700 * time.Millisecond)
	sendFrame(t, ws, ClientFrame{Type: FrameTypeStdin, Data: "export FOO=bar\n"})
	time.Sleep(300 * time.Millisecond)
	sendFrame(t, ws, ClientFrame{Type: FrameTypeStdin, Data: "echo value:$FOO\n"})
	time.Sleep(500 * time.Millisecond)
	sendFrame(t, ws, ClientFrame{Type: FrameTypeStdin, Data: "exit\n"})

	frames := collectFrames(t, ws, 15*time.Second)

	if out := stdoutOf(frames); !strings.Contains(out, "value:bar") {
		t.Fatalf("expected shell stdout to contain 'value:bar', got %q", out)
	}
	if code := exitCodeOf(t, frames); code != 0 {
		t.Fatalf("expected exit code 0 from shell exit, got %d", code)
	}
}

func TestExecShellModeResize(t *testing.T) {
	server, _ := newTestServer(t)
	ws := dialExec(t, server)

	sendFrame(t, ws, StartFrame{Type: FrameTypeStart, Cols: 100, Rows: 30})

	time.Sleep(700 * time.Millisecond)
	sendFrame(t, ws, ClientFrame{Type: FrameTypeResize, Cols: 132, Rows: 43})
	time.Sleep(300 * time.Millisecond)
	sendFrame(t, ws, ClientFrame{Type: FrameTypeStdin, Data: "stty size\n"})
	time.Sleep(500 * time.Millisecond)
	sendFrame(t, ws, ClientFrame{Type: FrameTypeStdin, Data: "exit\n"})

	frames := collectFrames(t, ws, 15*time.Second)

	if out := stdoutOf(frames); !strings.Contains(out, "43 132") {
		t.Fatalf("expected stty size to report '43 132' after resize, got %q", out)
	}
}

func TestExecRejectsNonStartFirstFrame(t *testing.T) {
	server, _ := newTestServer(t)
	ws := dialExec(t, server)

	sendFrame(t, ws, ClientFrame{Type: FrameTypeStdin, Data: "nope"})

	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("expected error frame, got read error: %v", err)
	}

	var frame serverFrame
	if err := json.Unmarshal(data, &frame); err != nil {
		t.Fatalf("invalid frame: %v", err)
	}
	if frame.Type != FrameTypeError {
		t.Fatalf("expected error frame, got %+v", frame)
	}
}

func TestExecConcurrentConnectionsAreIndependent(t *testing.T) {
	server, _ := newTestServer(t)

	type result struct {
		stdout   string
		exitCode int
		err      error
	}
	results := make(chan result, 2)

	for i := 0; i < 2; i++ {
		go func(i int) {
			url := "ws" + strings.TrimPrefix(server.URL, "http") + "/process/exec/connect"
			ws, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				results <- result{err: err}
				return
			}
			defer ws.Close()

			start, _ := json.Marshal(StartFrame{Type: FrameTypeStart, Command: "echo conn"})
			if err := ws.WriteMessage(websocket.TextMessage, start); err != nil {
				results <- result{err: err}
				return
			}

			var stdout strings.Builder
			exitCode := -1
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) && exitCode < 0 {
				_ = ws.SetReadDeadline(deadline)
				_, data, err := ws.ReadMessage()
				if err != nil {
					break
				}
				var frame serverFrame
				if err := json.Unmarshal(data, &frame); err != nil {
					continue
				}
				if frame.Type == FrameTypeStdout {
					stdout.WriteString(frame.Data)
				}
				if frame.Type == FrameTypeExit {
					exitCode = frame.ExitCode
				}
			}
			results <- result{stdout: stdout.String(), exitCode: exitCode}
		}(i)
	}

	for i := 0; i < 2; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("concurrent connection failed: %v", res.err)
			}
			if !strings.Contains(res.stdout, "conn") {
				t.Fatalf("expected 'conn' output, got %q", res.stdout)
			}
			if res.exitCode != 0 {
				t.Fatalf("expected exit code 0, got %d", res.exitCode)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("concurrent exec timed out")
		}
	}
}
