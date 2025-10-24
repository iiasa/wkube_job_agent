package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/iiasa/wkube-job-agent/services"
)

func abortIfCancelled(ctx context.Context, where string) error {
	if ctx.Err() != nil {
		return fmt.Errorf("context cancelled during %s — aborting", where)
	}

	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	services.Init(ctx, cancel)

	var errOccurred error
	var cmd *exec.Cmd

	// Signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigChan
		fmt.Fprintf(services.MultiLogWriter, "Received signal: %s — forwarding to child process\n", sig)
		if cmd != nil && cmd.Process != nil {
			syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) // Send to process group
		}
		cancel()
	}()

	defer func() {

		if err := services.PostProcessMappings(); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error in post-process-mappings: %v", err)
		}

		if err := services.VerboseResourceReport(); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "Error generating resource report: %v\n", err)
		}

		if err := services.UploadFile("/tmp/job.log", services.LogFileName); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error uploading job log: %v", err)

		}

		if r := recover(); r != nil {
			fmt.Fprintf(services.MultiLogWriter, "Panic: %v\nStack trace: %s\n", r, debug.Stack())
		} else if errOccurred != nil {
			if err := services.UpdateJobStatus("ERROR"); err != nil {
				fmt.Fprintf(services.MultiLogWriter, "Error updating status to ERROR: %v \n", err)
			}
			fmt.Fprintf(services.MultiLogWriter, "Error: %v \n", errOccurred)
		} else {

			if err := services.UpdateJobStatus("DONE"); err != nil {
				fmt.Fprintf(services.MultiLogWriter, "error updating status to DONE: %v", err)
			}
		}

		services.RemoteLogSink.FinalFlush()
	}()

	if len(os.Args) < 2 {
		errOccurred = fmt.Errorf("usage: go run main.go <command>")
		return
	}

	command := os.Args[1]
	// cmd = exec.Command("/bin/sh", "-c", command)
	cmd = exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	go func() {
		<-ctx.Done()
		if cmd != nil && cmd.Process != nil {
			pgid := -cmd.Process.Pid
			fmt.Printf("Context cancelled — killing process group %d\n", -pgid)
			syscall.Kill(pgid, syscall.SIGTERM)
			time.AfterFunc(10*time.Second, func() {
				syscall.Kill(pgid, syscall.SIGKILL)
			})
		}
	}()

	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
	cmd.Stdout = services.MultiLogWriter
	cmd.Stderr = services.MultiLogWriter

	if err := services.UpdateJobStatus("MAPPING_INPUTS"); err != nil {
		errOccurred = fmt.Errorf("error updating status to MAPPING_INPUTS: %v", err)
		return
	}

	if err := services.PreProcessMappings(); err != nil {
		errOccurred = fmt.Errorf("error in pre-process-mappings: %v", err)
		return
	}

	if err := abortIfCancelled(ctx, "input mappings"); err != nil {
		errOccurred = fmt.Errorf("%v", err)
		return
	}

	if err := services.UpdateJobStatus("PROCESSING"); err != nil {
		errOccurred = fmt.Errorf("error updating status to PROCESSING: %v", err)
		return
	}

	if err := services.ReportNodeName(); err != nil {
		errOccurred = fmt.Errorf("error reporting node name: %v", err)
		return
	}

	if socketAddress := os.Getenv("interactive_socket"); socketAddress != "" {
		tunnelErrCh := make(chan error, 1)
		services.StartTunnelWithRestart(ctx, socketAddress, tunnelErrCh)

		go func() {
			select {
			case err := <-tunnelErrCh:
				fmt.Fprintf(services.MultiLogWriter, "❌ Tunnel broke: %v — shutting down job\n", err)
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	if err := abortIfCancelled(ctx, "tunnel setup"); err != nil {
		errOccurred = fmt.Errorf("%v", err)
		return
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		errOccurred = fmt.Errorf("error starting command: %v", err)
		return
	}

	// Wait for command to complete
	if err := cmd.Wait(); err != nil {
		if ctx.Err() != nil {
			errOccurred = fmt.Errorf("Command interrupted due to context cancellation: %v\n", ctx.Err())
			return
		}
		errOccurred = fmt.Errorf("command execution error: %v", err)
		return
	}

	if err := services.UpdateJobStatus("MAPPING_OUTPUTS"); err != nil {
		errOccurred = fmt.Errorf("error updating status to MAPPING_OUTPUTS: %v", err)
		return
	}

}
