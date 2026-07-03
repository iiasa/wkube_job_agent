package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
)

type IPCRequest struct {
	Action  string          `json:"action"`
	Payload json.RawMessage `json:"payload"`
}

type IPCResponse struct {
	Status string `json:"status"`
	Result bool   `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read all request bytes until EOF
	reqBytes, err := io.ReadAll(conn)
	if err != nil {
		sendError(conn, fmt.Sprintf("failed to read from socket: %v", err))
		return
	}

	var req IPCRequest
	if err := json.Unmarshal(reqBytes, &req); err != nil {
		sendError(conn, fmt.Sprintf("invalid JSON format: %v", err))
		return
	}

	switch req.Action {
	case "register-validation-with-filename":
		var payload RegisterValidationPayload
		if err := json.Unmarshal(req.Payload, &payload); err != nil {
			sendError(conn, fmt.Sprintf("invalid payload: %v", err))
			return
		}

		result, err := RegisterValidationWithFilename(&payload)
		if err != nil {
			sendError(conn, fmt.Sprintf("validation registration failed: %v", err))
			return
		}

		sendSuccess(conn, result)

	case "get-access-token":
		accessToken, expiresAt, err := GetAccessToken()
		if err != nil {
			sendError(conn, fmt.Sprintf("failed to get token: %v", err))
			return
		}

		type TokenResponse struct {
			Status      string `json:"status"`
			AccessToken string `json:"access_token"`
			ExpiresAt   int64  `json:"expires_at"`
		}
		resp := TokenResponse{
			Status:      "success",
			AccessToken: accessToken,
			ExpiresAt:   expiresAt,
		}
		respBytes, _ := json.Marshal(resp)
		_, _ = conn.Write(respBytes)

	default:
		sendError(conn, fmt.Sprintf("unsupported action: %s", req.Action))
	}
}

func sendError(conn net.Conn, errMsg string) {
	resp := IPCResponse{
		Status: "error",
		Error:  errMsg,
	}
	respBytes, _ := json.Marshal(resp)
	_, _ = conn.Write(respBytes)
}

func sendSuccess(conn net.Conn, result bool) {
	resp := IPCResponse{
		Status: "success",
		Result: result,
	}
	respBytes, _ := json.Marshal(resp)
	_, _ = conn.Write(respBytes)
}

// StartIPCServer starts a Unix domain socket server listening for local requests from the child process.
func StartIPCServer(ctx context.Context) {
	socketPath := WagtSocketPath

	// Cleanup existing stale socket file
	_ = os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Fprintf(MultiLogWriter, "Error starting Unix socket listener: %v\n", err)
		return
	}
	defer listener.Close()

	// Set permission to allow access from child processes running as different UIDs
	if err := os.Chmod(socketPath, 0666); err != nil {
		fmt.Fprintf(MultiLogWriter, "Error setting socket permissions: %v\n", err)
		return
	}

	fmt.Fprintf(MultiLogWriter, "Unix socket IPC server listening on %s\n", socketPath)

	// Context cancellation cleanup
	go func() {
		<-ctx.Done()
		listener.Close()
		_ = os.Remove(socketPath)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check if listener was closed
			select {
			case <-ctx.Done():
				return
			default:
				fmt.Fprintf(MultiLogWriter, "Error accepting socket connection: %v\n", err)
				continue
			}
		}

		go handleConnection(conn)
	}
}
