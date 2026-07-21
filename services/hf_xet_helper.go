package services

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const HfXetHelperScript = `import sys
import json
import urllib.request
import urllib.error
import hashlib
import time
import os

class TokenRefresher:
    def __init__(self, cas_token, expires_at):
        self.cas_token = cas_token
        self.expires_at = expires_at
        try:
            self.project_slug = cas_token.split("xet_session_prj_")[1].split("_")[0]
        except Exception:
            self.project_slug = ""

    def refresh(self):
        import time
        import sys
        import socket
        import json

        # Refresh if token has less than 5 minutes (300 seconds) remaining
        if time.time() < self.expires_at - 300:
            return self.cas_token, self.expires_at

        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(30)
            s.connect("__WAGT_SOCK__")
            payload = json.dumps({"action": "get-access-token"}).encode("utf-8")
            s.sendall(payload)
            s.shutdown(socket.SHUT_WR)
            
            resp_bytes = s.recv(4096)
            s.close()
            
            resp = json.loads(resp_bytes.decode("utf-8"))
            if resp.get("status") == "success":
                access_token = resp.get("access_token")
                self.expires_at = resp.get("expires_at")
                self.cas_token = f"xet_session_prj_{self.project_slug}_{access_token}"
                print("Token successfully refreshed via Go IPC socket", file=sys.stderr)
            else:
                print(f"IPC token request failed: {resp.get('error')}", file=sys.stderr)
        except socket.timeout:
            print("Token refresh timed out after 30s waiting for IPC server", file=sys.stderr)
        except Exception as e:
            print(f"Token refresh failed in Python helper: {e}", file=sys.stderr)

        return self.cas_token, self.expires_at

def compute_sha256(filepath):
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()

def do_download(endpoint, cas_token, expires_at, files_json):
    import hf_xet
    files_list = json.loads(files_json)
    
    refresher = TokenRefresher(cas_token, expires_at)
    current_token, current_expiry = refresher.refresh()
    
    print(f"Starting download of {len(files_list)} files via hf_xet...", file=sys.stderr)
    
    total_size = sum(f.get("file_size", 0) for f in files_list)
    
    def progress_callback(group_report, item_reports):
        downloaded_bytes = group_report.total_bytes_completed
        if total_size > 0:
            percent = (downloaded_bytes / total_size) * 100
            print(f"Download progress: {percent:.2f}% ({downloaded_bytes}/{total_size} bytes)", file=sys.stderr)
        else:
            print(f"Download progress: {downloaded_bytes} bytes", file=sys.stderr)
            
    refresh_url = endpoint.split("/api/")[0] + "/api/v1/oauth/device/cas-token/"
    refresh_headers = {"Authorization": f"Bearer {current_token}"}

    session = hf_xet.XetSession()
    with session.new_file_download_group(
        endpoint=endpoint,
        token=current_token,
        token_expiry_unix_secs=current_expiry,
        token_refresh_url=refresh_url,
        token_refresh_headers=refresh_headers,
        progress_callback=progress_callback,
        progress_interval_ms=100
    ) as group:
        for f in files_list:
            info = hf_xet.XetFileInfo(f["hash"], f["file_size"])
            group.start_download_file(info, f["destination_path"])
            
    print("DOWNLOAD_SUCCESS")

def do_upload(project_slug, endpoint, cas_token, expires_at, register_url, files_json):
    import hf_xet
    files_list = json.loads(files_json)

    # Compute sha256 for every file BEFORE uploading so the hash is guaranteed
    # to match the bytes that will be read by upload_files.  Computing it after
    # upload creates a race: if the file is modified between the two reads the
    # registered sha256 would describe a different version than the uploaded CAS
    # content, producing a corrupted registration.
    pre_upload_sha256 = {f["local_path"]: compute_sha256(f["local_path"]) for f in files_list}

    refresher = TokenRefresher(cas_token, expires_at)
    current_token, current_expiry = refresher.refresh()

    refresh_url = endpoint.split("/api/")[0] + "/api/v1/oauth/device/cas-token/"
    refresh_headers = {"Authorization": f"Bearer {current_token}"}

    session = hf_xet.XetSession()
    with session.new_upload_commit(
        endpoint=endpoint,
        token=current_token,
        token_expiry_unix_secs=current_expiry,
        token_refresh_url=refresh_url,
        token_refresh_headers=refresh_headers,
    ) as commit:
        handles = []
        for f in files_list:
            h = commit.start_upload_file(f["local_path"], sha256=pre_upload_sha256[f["local_path"]])
            handles.append((f, h))
        
    registration_items = []
    for f, h in handles:
        upload_info = h.result()
        registration_items.append({
            "filename": f"{project_slug}/{f['remote_path']}",
            "merkle_hash": upload_info.xet_info.hash,
            "sha256": pre_upload_sha256[f["local_path"]],
            "file_size": upload_info.xet_info.file_size,
            "content_type": "application/octet-stream"
        })
    
    current_token, _ = refresher.refresh()
    payload = json.dumps({"items": registration_items}).encode("utf-8")
    req = urllib.request.Request(
        register_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Project-Slug": project_slug,
            "Authorization": f"Bearer {current_token}"
        },
        method="POST"
    )
    
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
            print(f"REGISTER_SUCCESS: {body}")
    except urllib.error.HTTPError as e:
        print(f"REGISTER_FAILED: {e.code} - {e.read().decode('utf-8')}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"REGISTER_FAILED: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Usage: hf_xet_helper.py [download|upload] ...", file=sys.stderr)
        sys.exit(1)
        
    cmd = sys.argv[1]
    if cmd == "download":
        endpoint = sys.argv[2]
        cas_token = sys.argv[3]
        expires_at = int(sys.argv[4])
        files_json = sys.argv[5]
        do_download(endpoint, cas_token, expires_at, files_json)
    elif cmd == "upload":
        project_slug = sys.argv[2]
        endpoint = sys.argv[3]
        cas_token = sys.argv[4]
        expires_at = int(sys.argv[5])
        register_url = sys.argv[6]
        files_json = sys.argv[7]
        do_upload(project_slug, endpoint, cas_token, expires_at, register_url, files_json)
    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
`

const RuntimeDir = "/mnt/tmp/.wkube_agent/xet_python"
const LocalDevDir = "/agent/xet_python"

// WriteHelperScript writes the helper python script to the temporary execution directory.
func WriteHelperScript() error {
	destDir := "/mnt/tmp/.wkube_agent"
	if _, err := os.Stat(destDir); os.IsNotExist(err) {
		destDir = "/agent" // Local dev fallback
	}
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create runtime dir: %w", err)
	}
	destPath := filepath.Join(destDir, "hf_xet_helper.py")
	script := strings.Replace(HfXetHelperScript, "__WAGT_SOCK__", WagtSocketPath, 1)
	return os.WriteFile(destPath, []byte(script), 0644)
}

// GetPythonInterpreter resolves the best python interpreter path pre-built in the container.
func GetPythonInterpreter() (string, error) {
	// 1. Check in the Kubernetes mounted agent shared volume
	targetPython := filepath.Join(RuntimeDir, "bin", "python3")
	if _, err := os.Stat(targetPython); err == nil {
		return targetPython, nil
	}

	// 2. Check in local development /agent folder fallback
	devPython := filepath.Join(LocalDevDir, "bin", "python3")
	if _, err := os.Stat(devPython); err == nil {
		return devPython, nil
	}

	return "", fmt.Errorf("pre-packaged standalone Python environment with hf_xet not found under %s or %s", RuntimeDir, LocalDevDir)
}


// RunHelperCommand executes the hf_xet helper python subcommand.
func RunHelperCommand(ctx context.Context, args []string) error {
	if err := WriteHelperScript(); err != nil {
		return err
	}

	pythonBin, err := GetPythonInterpreter()
	if err != nil {
		return err
	}

	destDir := "/mnt/tmp/.wkube_agent"
	if _, err := os.Stat(destDir); os.IsNotExist(err) {
		destDir = "/agent" // Local dev fallback
	}
	scriptPath := filepath.Join(destDir, "hf_xet_helper.py")
	fullArgs := append([]string{scriptPath}, args...)

	cmd := exec.CommandContext(ctx, pythonBin, fullArgs...)
	cmd.Stdout = MultiLogWriter
	cmd.Stderr = MultiLogWriter
	cmd.Env = append(os.Environ(),
		"HF_HOME=/mnt/tmp/hf_cache",
		// "RUST_LOG=debug",
		// "HF_XET_LOG_DEST=stderr",
	)

	return cmd.Run()
}
