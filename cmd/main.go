package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
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

func isMountPoint(path string) bool {
	stat, err := os.Lstat(path)
	if err != nil {
		return false
	}
	parentStat, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return false
	}
	
	statDev := stat.Sys().(*syscall.Stat_t).Dev
	parentDev := parentStat.Sys().(*syscall.Stat_t).Dev
	
	return statDev != parentDev
}

func waitForMount(ctx context.Context) error {
	// If the mountpoint directory doesn't exist, it wasn't configured for this pod.
	if _, err := os.Stat("/mnt/wdrv"); os.IsNotExist(err) {
		return nil
	}

	jobID := os.Getenv("JOB_ID")
	probeFile := filepath.Join("/mnt/wdrv", fmt.Sprintf(".wagt_init_probe_%s", jobID))

	fmt.Fprintln(services.MultiLogWriter, "Waiting for FUSE mount on /mnt/wdrv to become ready...")

	// Create a cleanup function for the probe file
	cleanup := func() {
		_ = os.Remove(probeFile)
	}
	defer cleanup()

	for i := 1; i <= 180; i++ {
		// First check if it's a mountpoint using pure Go device ID comparison
		if isMountPoint("/mnt/wdrv") {
			// Try writing to the probe file
			err := os.WriteFile(probeFile, []byte("wagt-probe-ok"), 0644)
			if err == nil {
				// Try reading it back
				data, err := os.ReadFile(probeFile)
				if err == nil && string(data) == "wagt-probe-ok" {
					fmt.Fprintf(services.MultiLogWriter, "FUSE mount /mnt/wdrv is ready and I/O works (attempt %d/180)\n", i)
					return nil
				}
			}
			fmt.Fprintf(services.MultiLogWriter, "  attempt %d/180: /mnt/wdrv is mounted but I/O probe failed (FUSE not ready yet), sleeping 5s...\n", i)
		} else {
			fmt.Fprintf(services.MultiLogWriter, "  attempt %d/180: /mnt/wdrv is not a mountpoint yet, sleeping 5s...\n", i)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}

	return fmt.Errorf("FATAL: FUSE mount /mnt/wdrv did not become I/O-ready within 900s (15 minutes)")
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
		
		podID := os.Getenv("POD_ID")
		if podID == "" {
			podID = "unknown"
		}
		evictedPath := fmt.Sprintf("/mnt/tmp/.wkube_agent/%s/evicted", podID)
		if err := os.WriteFile(evictedPath, []byte("evicted\n"), 0644); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "warning: could not touch evicted file: %v\n", err)
		}

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

		podID := os.Getenv("POD_ID")
		if podID == "" {
			podID = "unknown"
		}
		mainDonePath := fmt.Sprintf("/mnt/tmp/.wkube_agent/%s/main_done", podID)
		if err := os.WriteFile(mainDonePath, []byte("done\n"), 0644); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "warning: could not touch main_done file: %v\n", err)
		}

		cancel()
		services.RemoteLogSink.Wait()
		services.RemoteLogSink.FinalFlush()

		counterStr := strconv.Itoa(services.GetLogCounter()) + "\n"
		if err := os.WriteFile(services.LogCounterPath, []byte(counterStr), 0644); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error writing log counter: %v\n", err)
		}

		os.Exit(0)
	}()

	if shellPath, err := exec.LookPath("sh"); err == nil {
		cmd = exec.CommandContext(ctx, shellPath, "-c", command)
	} else {
		parts := strings.Fields(command)
		if len(parts) > 0 {
			cmd = exec.CommandContext(ctx, parts[0], parts[1:]...)
		} else {
			fmt.Fprintf(services.MultiLogWriter, "Error: empty command string\n")
			exitCode = 1
			return
		}
	}
	
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
	cmd.Stdout = services.MultiLogWriter
	cmd.Stderr = services.MultiLogWriter

	if err := services.UpdateJobStatus("MAPPING_INPUTS"); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "error updating status to MAPPING_INPUTS: %v\n", err)
		exitCode = 1
		return
	}

	if err := waitForMount(ctx); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "%v\n", err)
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
		if reportErr := services.VerboseResourceReport(); reportErr != nil {
			fmt.Fprintf(services.MultiLogWriter, "Error generating resource report: %v\n", reportErr)
		}
		return
	}

	exitCode = 0
	if reportErr := services.VerboseResourceReport(); reportErr != nil {
		fmt.Fprintf(services.MultiLogWriter, "Error generating resource report: %v\n", reportErr)
	}
}

func cmdFinalize() {
	exitCode := 1

	exitCodeRaw, err := os.ReadFile(services.JobExitCodePath)
	if err == nil {
		if n, err := strconv.Atoi(strings.TrimSpace(string(exitCodeRaw))); err == nil {
			exitCode = n
		}
	}

	counterRaw, err := os.ReadFile(services.LogCounterPath)
	if err == nil {
		cleaned := strings.TrimSpace(string(counterRaw))
		if n, err := strconv.Atoi(cleaned); err == nil {
			services.SetLogCounter(n)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Pass a dummy cancel function to services.Init in order to NOT cancel the finalizer context on health check failure.
	services.Init(ctx, func() {
		fmt.Fprintf(services.MultiLogWriter, "Health check failed: job is not healthy\n")
	})

	if reason := os.Getenv("WAGT_ABRUPT_TERMINATION"); reason != "" {
		if reason == "EVICTION" {
			fmt.Fprintf(services.MultiLogWriter, "[SYSTEM] Job was abruptly terminated due to Eviction (e.g., exceeded ephemeral storage), Timeout, or Node shutdown.\n")
		} else if reason == "OOM" {
			fmt.Fprintf(services.MultiLogWriter, "[SYSTEM] Job was abruptly terminated due to Out-of-Memory (OOMKilled).\n")
		}
	}

	if err := waitForMount(ctx); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "%v\n", err)
		services.RemoteLogSink.FinalFlush()
		os.Exit(1)
	}

	defer func() {
		counterStr := strconv.Itoa(services.GetLogCounter()) + "\n"
		if err := os.WriteFile(services.LogCounterPath, []byte(counterStr), 0644); err != nil {
			fmt.Fprintf(services.MultiLogWriter, "error writing final log counter: %v\n", err)
		}
	}()

	// Invalidate the FUSE cache for /mnt/wdrv upfront to ensure all subsequent
	// finalize steps (like UploadWdrvFilesCreatedByJobGid) see the latest files.
	services.InvalidateFUSECacheForPath("/mnt/wdrv")

	postProcessErr := services.PostProcessMappings()
	if postProcessErr != nil {
		fmt.Fprintf(services.MultiLogWriter, "error in post-process-mappings: %v\n", postProcessErr)
	}

	wdrvUploadErr := services.UploadWdrvFilesCreatedByJobGid()
	if wdrvUploadErr != nil {
		fmt.Fprintf(services.MultiLogWriter, "error in wdrv-gid-upload: %v\n", wdrvUploadErr)
	}


	if err := services.UpdateJobStatus("MAPPING_OUTPUTS"); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "error updating status to MAPPING_OUTPUTS: %v\n", err)
	}

	if err := services.ExecutePostTaskRegistry(); err != nil {
		fmt.Fprintf(services.MultiLogWriter, "error executing post task registry: %v\n", err)
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
