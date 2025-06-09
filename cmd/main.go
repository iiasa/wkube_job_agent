package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"

	"github.com/iiasa/wkube-job-agent/config"
	"github.com/iiasa/wkube-job-agent/services"
)

func main() {

	config.Init()

	var errOccurred error

	defer func() {

		if r := recover(); r != nil {
			fmt.Fprintf(config.MultiLogWriter, "Panic: %v\nStack trace: %s\n", r, debug.Stack())
		} else if errOccurred != nil {
			if err := config.UpdateJobStatus("ERROR"); err != nil {
				fmt.Fprintf(config.MultiLogWriter, "Error updating status to ERROR: %v \n", err)
			}
			fmt.Fprintf(config.MultiLogWriter, "Error: %v \n", errOccurred)
		}

		if err := config.RemoteLogSink.Send(); err != nil {
			fmt.Fprintf(os.Stdout, "Failed to flush final remaining logs to remote sink. %s", err)
		}

	}()

	if len(os.Args) < 2 {
		errOccurred = fmt.Errorf("usage: go run main.go <command>")
		return
	}

	command := os.Args[1]
	cmd := exec.Command("/bin/sh", "-c", command)

	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")

	cmd.Stdout = config.MultiLogWriter
	cmd.Stderr = config.MultiLogWriter

	if err := config.UpdateJobStatus("MAPPING_INPUTS"); err != nil {
		errOccurred = fmt.Errorf("error updating status to MAPPING_INPUTS: %v", err)
		return
	}

	if err := services.PreProcessMappings(); err != nil {
		errOccurred = fmt.Errorf("error in pre-process-mappings: %v", err)
		return
	}

	if err := config.UpdateJobStatus("PROCESSING"); err != nil {
		errOccurred = fmt.Errorf("error updating status to PROCESSING: %v", err)
		return
	}

	checkAndListDebugPath("BEFORE STARTING COMMAND")

	// Start command
	if err := cmd.Start(); err != nil {
		errOccurred = fmt.Errorf("error starting command: %v", err)
		return
	}

	// Wait for the command to finish
	if err := cmd.Wait(); err != nil {
		errOccurred = fmt.Errorf("command execution error: %v", err)
		return
	}

	if err := config.UpdateJobStatus("MAPPING_OUTPUTS"); err != nil {
		errOccurred = fmt.Errorf("error updating status to MAPPING_OUTPUTS: %v", err)
		return
	}

	if err := services.PostProcessMappings(); err != nil {
		errOccurred = fmt.Errorf("error in post-process-mappings: %v", err)
		return
	}

	checkAndListDebugPath("AFTER COMMAND FINISHED")

	if err := config.UpdateJobStatus("DONE"); err != nil {
		errOccurred = fmt.Errorf("error updating status to DONE: %v", err)
		return
	}

}

func checkAndListDebugPath(context string) {
	debugPath := os.Getenv("DEBUG_WKUBE_MAPPING_PATH")
	if debugPath == "" {
		return
	}

	fmt.Fprintf(config.MultiLogWriter, "DEBUG_WKUBE_MAPPING_PATH is set — listing %q (%s):\n", debugPath, context)

	info, err := os.Stat(debugPath)
	if os.IsNotExist(err) {
		fmt.Fprintf(config.MultiLogWriter, "%q does not exist\n", debugPath)
		return
	} else if err != nil {
		fmt.Fprintf(config.MultiLogWriter, "Error checking %q: %v\n", debugPath, err)
		return
	} else if !info.IsDir() {
		fmt.Fprintf(config.MultiLogWriter, "%q exists but is not a directory\n", debugPath)
		return
	}

	err = filepath.Walk(debugPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Fprintf(config.MultiLogWriter, "Error accessing %q: %v\n", path, err)
			return err
		}

		// Use Lstat to detect symlink
		lstatInfo, lerr := os.Lstat(path)
		if lerr != nil {
			fmt.Fprintf(config.MultiLogWriter, "Error lstat %q: %v\n", path, lerr)
			return lerr
		}

		mode := lstatInfo.Mode()

		switch {
		case mode&os.ModeSymlink != 0:
			// It's a symlink — print target
			target, terr := os.Readlink(path)
			if terr != nil {
				fmt.Fprintf(config.MultiLogWriter, "[LINK] %s -> (error reading link target: %v)\n", path, terr)
			} else {
				fmt.Fprintf(config.MultiLogWriter, "[LINK] %s -> %s\n", path, target)
			}
		case mode.IsDir():
			fmt.Fprintf(config.MultiLogWriter, "[DIR ] %s\n", path)
		case mode.IsRegular():
			fmt.Fprintf(config.MultiLogWriter, "[FILE] %s\n", path)
		default:
			fmt.Fprintf(config.MultiLogWriter, "[OTHER] %s (mode: %v)\n", path, mode)
		}

		return nil
	})

	if err != nil {
		fmt.Fprintf(config.MultiLogWriter, "Error walking %q: %v\n", debugPath, err)
	}
}
