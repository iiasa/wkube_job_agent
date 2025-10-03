package services

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"
)

func startReverseTunnel(localSocket string) error {
	sshUser := os.Getenv("TUNNEL_GATEWAY_SSH_USER")
	sshServer := os.Getenv("TUNNEL_GATEWAY_SSH_SERVER")
	sshPort := os.Getenv("TUNNEL_GATEWAY_SSH_PORT")

	if sshPort == "" {
		sshPort = "22" // default
	}

	tunnelGatewayDomain := os.Getenv("TUNNEL_GATEWAY_DOMAIN")
	sshKeyBase64 := os.Getenv("TUNNEL_GATEWAY_SSH_PRIVATE_KEY_BASE64")
	podID := os.Getenv("POD_ID")

	if sshUser == "" || sshServer == "" || sshKeyBase64 == "" || podID == "" || tunnelGatewayDomain == "" {
		return fmt.Errorf("missing required environment variables: SSH_USER, SSH_SERVER, SSH_PRIVATE_KEY, POD_ID")
	}

	// Decode base64 SSH key
	sshKeyBytes, err := base64.StdEncoding.DecodeString(sshKeyBase64)
	if err != nil {
		return fmt.Errorf("failed to decode SSH key: %v", err)
	}

	// Write SSH key to a secure temp file
	tmpKeyFile, err := os.CreateTemp("", "id_rsa_")
	if err != nil {
		return fmt.Errorf("failed to create temp key file: %v", err)
	}
	defer func() {
		tmpKeyFile.Close()
		os.Remove(tmpKeyFile.Name())
	}()

	if err := os.WriteFile(tmpKeyFile.Name(), []byte(sshKeyBytes), 0600); err != nil {
		return fmt.Errorf("failed to write SSH key to temp file: %v", err)
	}

	randomUUID := uuid.New()

	remoteSocketPath := "/tmp/" + randomUUID.String() + ".sock"

	var sshArgs []string
	sshArgs = append(sshArgs,
		"-i", tmpKeyFile.Name(),
		"-o", "StrictHostKeyChecking=no", // ⚠️ Replace in production
		"-o", "ExitOnForwardFailure=yes",
		"-o", "ServerAliveInterval=10", // 🔸 detect dead tunnel fast
		"-o", "ServerAliveCountMax=3", // 🔸 after 30s of no response, exit
		"-N", // Don't run remote command
		"-p", sshPort,
	)

	if strings.HasPrefix(localSocket, "unix:") {
		unixPath := strings.TrimPrefix(localSocket, "unix:")
		sshArgs = append(sshArgs, "-R", remoteSocketPath+":"+unixPath)
		fmt.Fprintf(MultiLogWriter, "Setting up UNIX → UNIX tunnel: %s -> %s \n", remoteSocketPath, unixPath)
	} else {
		sshArgs = append(sshArgs, "-R", remoteSocketPath+":"+localSocket)
		fmt.Fprintf(MultiLogWriter, "Setting up TCP → UNIX tunnel: %s -> %s \n", remoteSocketPath, localSocket)
	}

	sshArgs = append(sshArgs, sshUser+"@"+sshServer)

	cmd := exec.Command("/mnt/agent/ssh", sshArgs...)
	cmd.Stdout = MultiLogWriter
	cmd.Stderr = MultiLogWriter

	fmt.Fprintf(MultiLogWriter, "Starting reverse tunnel with command: ssh %s \n", strings.Join(sshArgs, " "))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start SSH reverse tunnel: %v", err)
	} else {
		fmt.Fprintf(MultiLogWriter, "Interactive socket tunneled at: %s.%s \n", randomUUID.String(), tunnelGatewayDomain)
	}

	return nil
}

func StartTunnelWithRestart(ctx context.Context, localSocket string, errCh chan<- error) {
	const (
		retryDelay        = 5 * time.Second
		gracePeriod       = 30 * time.Second
		maxConsecutiveErr = 5
	)

	go func() {
		consecutiveFails := 0
		var firstFailAt time.Time

		for {
			select {
			case <-ctx.Done():
				fmt.Fprintln(MultiLogWriter, "Tunnel goroutine exiting due to context cancellation")
				return
			default:
				err := startReverseTunnel(localSocket)
				if err != nil {
					fmt.Fprintf(MultiLogWriter, "Tunnel process exited with error: %v\n", err)
					consecutiveFails++

					if consecutiveFails == 1 {
						firstFailAt = time.Now()
					}

					// Check if grace period or max retries exceeded
					if time.Since(firstFailAt) > gracePeriod || consecutiveFails >= maxConsecutiveErr {
						errCh <- fmt.Errorf("tunnel failed %d times over %s — giving up", consecutiveFails, time.Since(firstFailAt))
						return
					}

					fmt.Fprintf(MultiLogWriter, "Retrying tunnel in %s (%d/%d)\n", retryDelay, consecutiveFails, maxConsecutiveErr)
					time.Sleep(retryDelay)
					continue
				}

				// Reset on success
				consecutiveFails = 0
			}
		}
	}()
}
