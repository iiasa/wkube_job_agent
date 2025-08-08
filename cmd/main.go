package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"

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
		if r := recover(); r != nil {
			fmt.Fprintf(services.MultiLogWriter, "Panic: %v\nStack trace: %s\n", r, debug.Stack())
		} else if errOccurred != nil {
			if err := services.UpdateJobStatus("ERROR"); err != nil {
				fmt.Fprintf(services.MultiLogWriter, "Error updating status to ERROR: %v \n", err)
			}
			fmt.Fprintf(services.MultiLogWriter, "Error: %v \n", errOccurred)
		}

		if err := services.PostProcessMappings(); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error in post-process-mappings: %v", err)
		}

		if err := services.VerboseResourceReport(); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "Error generating resource report: %v\n", err)
		}

		if err := services.UploadFile("/tmp/job.log", services.LogFileName); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error uploading job log: %v", err)

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

	checkAndListDebugPath("BEFORE STARTING COMMAND")

	if socketAddress := os.Getenv("interactive_socket"); socketAddress != "" {
		err := services.StartTunnelWithRestart(socketAddress)
		errOccurred = fmt.Errorf("error setting up interactive tunnel: %v", err)
		return
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
			fmt.Fprintf(services.MultiLogWriter, "Command interrupted due to context cancellation: %v\n", ctx.Err())
			return
		}

		errOccurred = fmt.Errorf("command execution error: %v", err)
		return
	}

	if err := services.UpdateJobStatus("MAPPING_OUTPUTS"); err != nil {
		errOccurred = fmt.Errorf("error updating status to MAPPING_OUTPUTS: %v", err)
		return
	}

	checkAndListDebugPath("AFTER COMMAND FINISHED")

	if err := services.UpdateJobStatus("DONE"); err != nil {
		errOccurred = fmt.Errorf("error updating status to DONE: %v", err)
		return
	}
}

func checkAndListDebugPath(context string) {
	debugPath := os.Getenv("DEBUG_WKUBE_MAPPING_PATH")
	if debugPath == "" {
		return
	}

	fmt.Fprintf(services.MultiLogWriter, "DEBUG_WKUBE_MAPPING_PATH is set — listing %q (%s):\n", debugPath, context)

	info, err := os.Stat(debugPath)
	if os.IsNotExist(err) {
		fmt.Fprintf(services.MultiLogWriter, "%q does not exist\n", debugPath)
		return
	} else if err != nil {
		fmt.Fprintf(services.MultiLogWriter, "Error checking %q: %v\n", debugPath, err)
		return
	} else if !info.IsDir() {
		fmt.Fprintf(services.MultiLogWriter, "%q exists but is not a directory\n", debugPath)
		return
	}

	err = filepath.Walk(debugPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Fprintf(services.MultiLogWriter, "Error accessing %q: %v\n", path, err)
			return err
		}

		// Use Lstat to detect symlink
		lstatInfo, lerr := os.Lstat(path)
		if lerr != nil {
			fmt.Fprintf(services.MultiLogWriter, "Error lstat %q: %v\n", path, lerr)
			return lerr
		}

		mode := lstatInfo.Mode()

		switch {
		case mode&os.ModeSymlink != 0:
			// It's a symlink — print target
			target, terr := os.Readlink(path)
			if terr != nil {
				fmt.Fprintf(services.MultiLogWriter, "[LINK] %s -> (error reading link target: %v)\n", path, terr)
			} else {
				fmt.Fprintf(services.MultiLogWriter, "[LINK] %s -> %s\n", path, target)
			}
		case mode.IsDir():
			fmt.Fprintf(services.MultiLogWriter, "[DIR ] %s\n", path)
		case mode.IsRegular():
			fmt.Fprintf(services.MultiLogWriter, "[FILE] %s\n", path)
		default:
			fmt.Fprintf(services.MultiLogWriter, "[OTHER] %s (mode: %v)\n", path, mode)
		}

		return nil
	})

	if err != nil {
		fmt.Fprintf(services.MultiLogWriter, "Error walking %q: %v\n", debugPath, err)
	}
}
