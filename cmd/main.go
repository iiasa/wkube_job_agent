package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime/debug"
	"strconv"
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

func cmdRun(command string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	services.Init(ctx, cancel)

	go services.StartIPCServer(ctx)

	var cmd *exec.Cmd

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigChan
		fmt.Fprintf(services.MultiLogWriter, "Received signal: %s — forwarding to child process\n", sig)
		if cmd != nil && cmd.Process != nil {
			syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		}
		cancel()
	}()

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

	exitCode := 0

	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(services.MultiLogWriter, "Panic: %v\nStack trace: %s\n", r, debug.Stack())
			exitCode = 1
		}

		if err := os.WriteFile(services.JobExitCodePath, []byte(strconv.Itoa(exitCode)+"\n"), 0644); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error writing exit code: %v\n", err)
		}

		services.RemoteLogSink.FinalFlush()

		counterStr := strconv.Itoa(services.GetLogCounter()) + "\n"
		if err := os.WriteFile(services.LogCounterPath, []byte(counterStr), 0644); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error writing log counter: %v\n", err)
		}

		os.Exit(0)
	}()

	cmd = exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
	cmd.Stdout = services.MultiLogWriter
	cmd.Stderr = services.MultiLogWriter

	if err := services.UpdateJobStatus("MAPPING_INPUTS"); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "error updating status to MAPPING_INPUTS: %v\n", err)
		exitCode = 1
		return
	}

	if err := services.PreProcessMappings(); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "error in pre-process-mappings: %v\n", err)
		exitCode = 1
		return
	}

	if err := abortIfCancelled(ctx, "input mappings"); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "%v\n", err)
		exitCode = 1
		return
	}

	if err := services.UpdateJobStatus("PROCESSING"); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "error updating status to PROCESSING: %v\n", err)
		exitCode = 1
		return
	}

	if err := services.ReportNodeName(); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "error reporting node name: %v\n", err)
	}

	if socketAddress := os.Getenv("interactive_socket"); socketAddress != "" {
		tunnelErrCh := make(chan error, 1)
		services.StartTunnelWithRestart(ctx, socketAddress, tunnelErrCh)

		go func() {
			select {
			case err := <-tunnelErrCh:
				fmt.Fprintf(services.MultiLogWriter, "Tunnel broke: %v — shutting down job\n", err)
				cancel()
			case <-ctx.Done():
			}
		}()
	}

	if err := abortIfCancelled(ctx, "tunnel setup"); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "%v\n", err)
		exitCode = 1
		return
	}

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "error starting command: %v\n", err)
		exitCode = 1
		return
	}

	if err := cmd.Wait(); err != nil {
		if ctx.Err() != nil {
			fmt.Fprintf(services.MultiLogWriter, "Command interrupted due to context cancellation: %v\n", ctx.Err())
		} else {
			fmt.Fprintf(services.MultiLogWriter, "command execution error: %v\n", err)
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = 1
		}
		return
	}

	exitCode = 0
}

func cmdFinalize() {
	exitCode := 1

	exitCodeRaw, err := os.ReadFile(services.JobExitCodePath)
	if err == nil {
		if n, err := strconv.Atoi(string(exitCodeRaw)); err == nil {
			exitCode = n
		}
	}

	counterRaw, err := os.ReadFile(services.LogCounterPath)
	if err == nil {
		if n, err := strconv.Atoi(string(counterRaw)); err == nil {
			services.SetLogCounter(n)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	services.Init(ctx, cancel)

	postProcessErr := services.PostProcessMappings()
	if postProcessErr != nil {
		fmt.Fprintf(services.MultiLogWriter, "error in post-process-mappings: %v\n", postProcessErr)
	}

	wdrvUploadErr := services.UploadWdrvFilesCreatedByJobUid()
	if wdrvUploadErr != nil {
		fmt.Fprintf(services.MultiLogWriter, "error in wdrv-uid-upload: %v\n", wdrvUploadErr)
	}

	if err := services.VerboseResourceReport(); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "Error generating resource report: %v\n", err)
	}

	if err := services.UpdateJobStatus("MAPPING_OUTPUTS"); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "error updating status to MAPPING_OUTPUTS: %v\n", err)
	}

	if exitCode == 0 && postProcessErr == nil && wdrvUploadErr == nil {
		if err := services.UpdateJobStatus("DONE"); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error updating status to DONE: %v\n", err)
		}
	} else {
		if err := services.UpdateJobStatus("ERROR"); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error updating status to ERROR: %v\n", err)
		}
	}

	projectSlug := services.GetProjectSlug()
	if projectSlug != "" {
		if err := services.RemotePushLog(services.JobLogPath, services.LogFileName, projectSlug); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error uploading job log via xet: %v\n", err)
		}
	} else {
		fmt.Fprintln(services.MultiLogWriter, "warning: project slug not found, attempting standard upload")
		if err := services.UploadFile(services.JobLogPath, services.LogFileName); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error uploading job log: %v\n", err)
		}
	}

	services.RemoteLogSink.FinalFlush()

	os.Exit(exitCode)
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  wagt run \"<command>\"       — Phase 1: init → IPC → input mapping → run command\n")
	fmt.Fprintf(os.Stderr, "  wagt finalize              — Phase 2: output mapping → resource report → status → push log\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "run":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: wagt run \"<command>\"")
			os.Exit(1)
		}
		cmdRun(os.Args[2])
	case "finalize":
		cmdFinalize()
	default:
		usage()
	}
}
