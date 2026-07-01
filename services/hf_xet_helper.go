package services

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)


// GetHelperBinary resolves the path to the standalone static hf_xet_helper binary.
func GetHelperBinary() (string, error) {
	// 1. Check in the Kubernetes mounted agent shared volume
	targetBin := "/mnt/tmp/.wkube_agent/hf_xet_helper"
	if _, err := os.Stat(targetBin); err == nil {
		return targetBin, nil
	}

	// 2. Check in local development /agent folder fallback
	devBin := "/agent/hf_xet_helper"
	if _, err := os.Stat(devBin); err == nil {
		return devBin, nil
	}

	return "", fmt.Errorf("standalone hf_xet_helper binary not found under /mnt/tmp/.wkube_agent or /agent")
}

// RunHelperCommand executes the hf_xet helper subcommand directly via the static binary.
func RunHelperCommand(ctx context.Context, args []string) error {
	helperBin, err := GetHelperBinary()
	if err != nil {
		return err
	}

	tmpDir := "/mnt/tmp"
	if _, err := os.Stat(tmpDir); err != nil {
		return fmt.Errorf("required directory %s does not exist: %v", tmpDir, err)
	}

	staticxTmpDir := "/mnt/tmp/.wkube_agent/tmp"
	if err := os.MkdirAll(staticxTmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create staticx tmp dir: %v", err)
	}

	uniqueTmpDir, err := os.MkdirTemp(staticxTmpDir, "hfxet-*")
	if err != nil {
		return fmt.Errorf("failed to create unique temp dir: %v", err)
	}
	defer os.RemoveAll(uniqueTmpDir)

	var caBundlePath string
	srcBundle := "/mnt/tmp/.wkube_agent/ca_bundle.pem"
	destBundle := filepath.Join(staticxTmpDir, "ca_bundle.pem")
	if _, err := os.Stat(srcBundle); err == nil {
		input, err := os.ReadFile(srcBundle)
		if err != nil {
			return fmt.Errorf("failed to read ca_bundle: %v", err)
		}
		if err := os.WriteFile(destBundle, input, 0644); err != nil {
			return fmt.Errorf("failed to write ca_bundle copy: %v", err)
		}
		caBundlePath = destBundle
	}

	localHelperBin := filepath.Join(uniqueTmpDir, "hf_xet_helper")
	binData, err := os.ReadFile(helperBin)
	if err != nil {
		return fmt.Errorf("failed to read helper binary: %v", err)
	}
	if err := os.WriteFile(localHelperBin, binData, 0755); err != nil {
		return fmt.Errorf("failed to write local helper binary copy: %v", err)
	}

	cmd := exec.CommandContext(ctx, localHelperBin, args...)
	cmd.Stdout = MultiLogWriter
	cmd.Stderr = MultiLogWriter
	cmd.Env = append(os.Environ(),
		"HF_HOME="+filepath.Join(tmpDir, "hf_cache"),
		"TMPDIR="+uniqueTmpDir,
		"TEMP="+uniqueTmpDir,
		"TMP="+uniqueTmpDir,
	)
	if caBundlePath != "" {
		cmd.Env = append(cmd.Env,
			"SSL_CERT_FILE="+caBundlePath,
			"REQUESTS_CA_BUNDLE="+caBundlePath,
			"CURL_CA_BUNDLE="+caBundlePath,
		)
	}

	return cmd.Run()
}
