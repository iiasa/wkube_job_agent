package services

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/iiasa/wkube-job-agent/config"
)

func startReverseTunnel(localSocket string) error {
	sshUser := os.Getenv("SSH_USER")
	sshServer := os.Getenv("SSH_SERVER")
	tunnelGatewayDomain := os.Getenv("TUNNEL_GATEWAY_DOMAIN")
	sshKey := os.Getenv("SSH_PRIVATE_KEY")
	podID := os.Getenv("POD_ID")

	if sshUser == "" || sshServer == "" || sshKey == "" || podID == "" || tunnelGatewayDomain == "" {
		return fmt.Errorf("missing required environment variables: SSH_USER, SSH_SERVER, SSH_PRIVATE_KEY, POD_ID")
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

	if err := os.WriteFile(tmpKeyFile.Name(), []byte(sshKey), 0600); err != nil {
		return fmt.Errorf("failed to write SSH key to temp file: %v", err)
	}

	remoteSocketPath := "/tmp/" + podID + ".sock"

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
		fmt.Fprintf(config.MultiLogWriter, "Setting up UNIX → UNIX tunnel: %s -> %s \n", remoteSocketPath, unixPath)
	} else {
		sshArgs = append(sshArgs, "-R", remoteSocketPath+":"+localSocket)
		fmt.Fprintf(config.MultiLogWriter, "Setting up TCP → UNIX tunnel: %s -> %s", remoteSocketPath, localSocket)
	}

	sshArgs = append(sshArgs, sshUser+"@"+sshServer)

	cmd := exec.Command("/mnt/agent/ssh", sshArgs...)
	cmd.Stdout = config.MultiLogWriter
	cmd.Stderr = config.MultiLogWriter

	fmt.Fprintf(config.MultiLogWriter, "Starting reverse tunnel with command: ssh %s", strings.Join(sshArgs, " "))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start SSH reverse tunnel: %v", err)
	} else {
		fmt.Fprintf(config.MultiLogWriter, "Interactive socket tunneled at: %s.%s", podID, tunnelGatewayDomain)
	}

	return nil
}

func StartTunnelWithRestart(localSocket string) error {
	errCh := make(chan error, 1)
	go func() {
		for {
			fmt.Fprintln(config.MultiLogWriter, "Starting reverse tunnel...")
			err := startReverseTunnel(localSocket) // return proper error to capture error..refer other implementation
			if err != nil {

				errCh <- fmt.Errorf("tunnel exited with error: %v", err)
				return // exit goroutine
			} else {

				fmt.Fprintln(config.MultiLogWriter, "Tunnel exited normally")
			}

			// Optional: add backoff delay before retry
			time.Sleep(5 * time.Second)
		}
	}()

	return <-errCh
}
