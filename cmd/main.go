package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/iiasa/wkube-job-agent/config"
	"github.com/iiasa/wkube-job-agent/services"
)

func main() {

	config.Init()

	var errOccurred error

	defer func() {
		if errOccurred != nil {
			if err := config.UpdateJobStatus("ERROR"); err != nil {
				fmt.Fprintf(config.MultiLogWriter, "Error updating status to ERROR: %v \n", err)
			}
			fmt.Fprintf(config.MultiLogWriter, "Error: %v \n", errOccurred)
		}

		if err := config.RemoteLogSink.Send(); err != nil {
			fmt.Fprintf(os.Stdout, "Failed to flush final remaining logs to remote sink.")
		}

	}()

	if len(os.Args) < 2 {
		errOccurred = fmt.Errorf("usage: go run main.go <command>")
		return
	}

	command := os.Args[1]
	cmd := exec.Command("/bin/sh", "-c", command)

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

	if err := config.UpdateJobStatus("DONE"); err != nil {
		errOccurred = fmt.Errorf("error updating status to DONE: %v", err)
		return
	}

}
