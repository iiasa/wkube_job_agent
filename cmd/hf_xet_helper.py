import sys
import json
import urllib.request
import urllib.error
import hashlib
import time
import os

# Ensure SSL_CERT_FILE is set before any hf_xet import so that Rust's
# rustls-native-certs picks it up when it initialises the HTTP client.
_CERT_SEARCH_PATHS = [
    "/mnt/tmp/.wkube_agent/tmp/ca_bundle.pem",
    "/mnt/tmp/.wkube_agent/ca_bundle.pem",
    "/agent/ca_bundle.pem",
]
for _p in _CERT_SEARCH_PATHS:
    if os.path.isfile(_p):
        os.environ.setdefault("SSL_CERT_FILE", _p)
        os.environ.setdefault("REQUESTS_CA_BUNDLE", _p)
        os.environ.setdefault("CURL_CA_BUNDLE", _p)
        break


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
            s.connect("/mnt/tmp/.wkube_agent/wagt.sock")
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
    download_infos = []
    for f in files_list:
        download_infos.append(
            hf_xet.PyXetDownloadInfo(
                destination_path=f["destination_path"],
                hash=f["hash"],
                file_size=f["file_size"]
            )
        )
    
    refresher = TokenRefresher(cas_token, expires_at)
    
    print(f"Starting download of {len(download_infos)} files via hf_xet...", file=sys.stderr)
    
    total_size = sum(f.get("file_size", 0) for f in files_list)
    
    def progress_updater(downloaded_bytes):
        if total_size > 0:
            percent = (downloaded_bytes / total_size) * 100
            print(f"Download progress: {percent:.2f}% ({downloaded_bytes}/{total_size} bytes)", file=sys.stderr)
        else:
            print(f"Download progress: {downloaded_bytes} bytes", file=sys.stderr)
    
    hf_xet.download_files(
        files=download_infos,
        endpoint=endpoint,
        token_info=(cas_token, expires_at),
        token_refresher=refresher.refresh,
        progress_updater=[progress_updater],
        request_headers=None
    )
    print("DOWNLOAD_SUCCESS")

def do_upload(project_slug, endpoint, cas_token, expires_at, register_url, files_json):
    import hf_xet
    files_list = json.loads(files_json)
    local_paths = [f["local_path"] for f in files_list]

    refresher = TokenRefresher(cas_token, expires_at)

    upload_results = hf_xet.upload_files(
        file_paths=local_paths,
        endpoint=endpoint,
        token_info=(cas_token, expires_at),
        token_refresher=refresher.refresh,
        progress_updater=None,
        _repo_type=None,
        request_headers=None,
        sha256s=None,
        skip_sha256=False
    )

    
    registration_items = []
    for f, upload_info in zip(files_list, upload_results):
        sha256_hash = compute_sha256(f["local_path"])
        registration_items.append({
            "filename": f"{project_slug}/{f['remote_path']}",
            "merkle_hash": upload_info.hash,
            "sha256": sha256_hash,
            "file_size": upload_info.file_size,
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
