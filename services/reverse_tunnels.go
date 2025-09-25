package services

import (
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
		"-N", // Don't run remote command
	)

	if strings.HasPrefix(localSocket, "unix:") {
		unixPath := strings.TrimPrefix(localSocket, "unix:")
		sshArgs = append(sshArgs, "-R", remoteSocketPath+":"+unixPath)
		fmt.Fprintf(MultiLogWriter, "Setting up UNIX → UNIX tunnel: %s -> %s \n", remoteSocketPath, unixPath)
	} else {
		sshArgs = append(sshArgs, "-R", remoteSocketPath+":"+localSocket)
		fmt.Fprintf(MultiLogWriter, "Setting up TCP → UNIX tunnel: %s -> %s", remoteSocketPath, localSocket)
	}

	sshArgs = append(sshArgs, sshUser+"@"+sshServer)

	cmd := exec.Command("/mnt/agent/ssh", sshArgs...)
	cmd.Stdout = MultiLogWriter
	cmd.Stderr = MultiLogWriter

	fmt.Fprintf(MultiLogWriter, "Starting reverse tunnel with command: ssh %s", strings.Join(sshArgs, " "))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start SSH reverse tunnel: %v", err)
	} else {
		fmt.Fprintf(MultiLogWriter, "Interactive socket tunneled at: %s.%s", randomUUID.String(), tunnelGatewayDomain)
	}

	return nil
}

func StartTunnelWithRestart(localSocket string) error {
	errCh := make(chan error, 1)
	go func() {
		for {
			fmt.Fprintln(MultiLogWriter, "Starting reverse tunnel...")
			err := startReverseTunnel(localSocket) // return proper error to capture error..refer other implementation
			if err != nil {

				errCh <- fmt.Errorf("tunnel exited with error: %v", err)
				return // exit goroutine
			} else {

				fmt.Fprintln(MultiLogWriter, "Tunnel exited normally")
			}

			// Optional: add backoff delay before retry
			time.Sleep(5 * time.Second)
		}
	}()

	return <-errCh
}
