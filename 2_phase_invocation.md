# Plan: Split wkube-job-agent into `run` and `finalize` phases

## Goal
Split the single-process `wagt` job agent into two separate invocations:
- **Phase 1 (`run`)**: init → IPC server → input mapping → run the command → exit. No output mapping, no log push, no DONE/ERROR status.
- **Phase 2 (`finalize`)**: output mapping → resource report → DONE/ERROR status → log push → exit with the command's real exit code.

Phase 2 runs in the existing `wkube-agent-puller` sidecar (under `containers`), which k8s starts only after the phase-1 initContainer completes.

## Decisions (locked)
1. **Execution model**: Phase 1 = job initContainer; Phase 2 = `wkube-agent-puller` sidecar in `containers`. k8s guarantees the sidecar starts only after the initContainer finishes.
2. **Failure propagation**: Phase 1 always exits `0` after writing the real command exit code to a shared file, so the sidecar always runs. Phase 2 reads that code, sets `DONE`/`ERROR`, pushes logs/outputs, and exits with the original code (pod/Job success still reflects the command).
3. **Mode selection**: subcommand — `wagt run "<command>"` and `wagt finalize`.
4. **Counter preservation**: phase 1 writes the final `logCounter` to a shared file after `FinalFlush`; phase 2 seeds `logCounter` from it before constructing `RemoteLogSink`, so live-stream chunk numbering (`wkube0000000N`) continues seamlessly.
5. **Shared log path**: `/mnt/tmp/.wkube_agent/job.log` (existing shared `agent-volume` emptyDir), replacing `/tmp/job.log`.

## Shared state (on `agent-volume` emptyDir at `/mnt/tmp/.wkube_agent`, mounted in both containers)
- `/mnt/tmp/.wkube_agent/wagt` — the binary (copied by the puller image ENTRYPOINT `cp -rp /agent/* /mnt/tmp/.wkube_agent/`).
- `/mnt/tmp/.wkube_agent/job.log` — full job log (phase 1 creates; phase 2 appends).
- `/mnt/tmp/.wkube_agent/exit_code` — command exit code written by phase 1, read by phase 2.
- `/mnt/tmp/.wkube_agent/log_counter` — final `logCounter` written by phase 1, read by phase 2.

## Data flow
```
initContainer[0]: wkube-agent-puller      -> ENTRYPOINT copies /agent/* -> /mnt/tmp/.wkube_agent/
initContainer[1]: job (wagt run "<cmd>")
   Init (clients, RemoteLogSink, open job.log)
   StartIPCServer (/tmp/wagt.sock)  [child registers validations/tokens]
   status MAPPING_INPUTS -> PreProcessMappings
   status PROCESSING -> ReportNodeName -> tunnel (if interactive_socket)
   cmd.Start()/cmd.Wait()
   extract exit code -> write /mnt/tmp/.wkube_agent/exit_code
   FinalFlush (stream remaining chunks)
   write /mnt/tmp/.wkube_agent/log_counter
   exit 0                       # always, so sidecar runs
container[0]: wkube-agent-puller sidecar (wagt finalize)
   read /mnt/tmp/.wkube_agent/exit_code, /mnt/tmp/.wkube_agent/log_counter
   seed logCounter; Init (clients, RemoteLogSink, append job.log)
   status MAPPING_OUTPUTS -> PostProcessMappings
   VerboseResourceReport
   status DONE (exit_code==0) | ERROR (else or post-process failed)
   RemotePushLog(/mnt/tmp/.wkube_agent/job.log)
   FinalFlush
   exit(exit_code)
```

## Changes

### 1. `services/config.go`
- Introduce package vars:
  - `JobLogPath = getenvWithDefault("WAGT_JOB_LOG_PATH", "/mnt/tmp/.wkube_agent/job.log")`
  - `JobExitCodePath = getenvWithDefault("WAGT_EXIT_CODE_PATH", "/mnt/tmp/.wkube_agent/exit_code")`
  - `LogCounterPath = getenvWithDefault("WAGT_LOG_COUNTER_PATH", "/mnt/tmp/.wkube_agent/log_counter")`
- In `Init`, open `JobLogPath` (not `/tmp/job.log`) with the existing `O_CREATE|O_WRONLY|O_APPEND` flags. `Init` is unchanged otherwise — both phases use `RemoteLogSink` in `MultiLogWriter`.
- No mode parameter needed (counter seeding happens in `main` before `Init`).

### 2. `services/logsink.go`
- Add accessor helpers (guarded by existing `counterMu`):
  - `func GetLogCounter() int`
  - `func SetLogCounter(n int)`
- These let phase 2 seed the counter before `NewRemoteLogger` runs inside `Init`.

### 3. `cmd/main.go`
Refactor `main()` into subcommand dispatch on `os.Args[1]`:
- `wagt run "<command>"` (`os.Args[2]` is the command) — current flow **minus** the defer block (no `PostProcessMappings`, no `VerboseResourceReport`, no `UpdateJobStatus(DONE/ERROR)`, no `RemotePushLog`, no `FinalFlush` in defer). Instead, after `cmd.Wait()`:
  - Extract exit code from `cmd.Wait()` error (`exec.ExitError` → `ExitCode`; signal/context cancel → non-zero, e.g. 137 for SIGTERM or 1).
  - Write exit code to `JobExitCodePath` (always, even on panic via `recover`).
  - `RemoteLogSink.FinalFlush()`.
  - Write `GetLogCounter()` to `LogCounterPath`.
  - `os.Exit(0)` always (success of phase 1 = it ran; command success is communicated via `exit_code`).
  - Keep signal handler, `abortIfCancelled`, tunnel, IPC server, `MAPPING_INPUTS`/`PROCESSING` statuses, `ReportNodeName`.
  - Do NOT set `MAPPING_OUTPUTS` (phase 2 owns it).
- `wagt finalize` — no `os.Args[2]`:
  - Read `JobExitCodePath` → `exitCode` (missing/unparseable → treat as `1`).
  - Read `LogCounterPath` → `SetLogCounter(n)` (missing → `0`).
  - `Init(ctx, cancel)`.
  - `UpdateJobStatus("MAPPING_OUTPUTS")`.
  - `PostProcessMappings()` (capture error).
  - `VerboseResourceReport()` (best-effort, log on error).
  - If `exitCode == 0` AND post-process OK → `UpdateJobStatus("DONE")`; else `UpdateJobStatus("ERROR")`.
  - `RemotePushLog(JobLogPath, LogFileName, projectSlug)` (fallback `UploadFile` if no slug, as today).
  - `RemoteLogSink.FinalFlush()`.
  - `os.Exit(exitCode)` (so a failed command or failed post-process makes the pod fail).
  - No IPC server, no tunnel, no child command.
- Unknown/missing subcommand → print usage, exit non-zero.

### 4. `/app/scheduler/k8_gateway_actions/dispatch_build_and_push.py` (`launch_k8_job`)
- `main_container_shell_script`: change the binary invocation from
  `"$binary_file" "%s"` to `"$binary_file" run "%s"`.
  - Since `wagt run` always exits 0, simplify the exit handling: keep the SIGTERM trap (forward to `wagt`, which forwards to the child process group), but `exit 0` after `wait`. Remove the `exit "$EXIT_CODE"` propagation (the sidecar now owns the real exit code). Remove or shorten the `sleep 30` (no longer needed for log flush since the sidecar performs the final push); keep a short grace sleep only if desired.
- Compose `flush_created_files_command` (currently undefined) for the sidecar:
  ```python
  flush_created_files_command = [
      "/bin/sh", "-c",
      "chmod +x /mnt/tmp/.wkube_agent/wagt && /mnt/tmp/.wkube_agent/wagt finalize"
  ]
  ```
  - The sidecar already mounts `/mnt/tmp/.wkube_agent` (agent-volume), `/mnt/tmp`, and `/mnt/wdrv`, so it can read `job.log`/`exit_code`/`log_counter` and access output-mapping sources.
  - The puller image ENTRYPOINT is `["sh","-c",...]`, so `/bin/sh` is available in the sidecar.

## Failure modes & edge cases
- **Command fails (non-zero)**: phase 1 writes the non-zero code and exits 0 → sidecar runs → `ERROR` status, outputs/logs pushed, sidecar exits non-zero → Job/pod fails. ✔
- **Command killed by SIGTERM**: phase 1 forwards to child process group, writes non-zero exit code (137/1), exits 0 → sidecar runs normally. ✔
- **Phase 1 panic/crash before writing `exit_code`**: `recover` in defer writes a non-zero code; if even that fails, phase 2 treats missing file as `1` → `ERROR`. Logs may be incomplete. Acceptable.
- **Missing `log_counter`**: phase 2 seeds `0`; possible chunk-name overlap with phase 1's already-flushed chunks (live-stream only; the authoritative full-log upload via `RemotePushLog` is unaffected). Acceptable.
- **Post-process fails after command succeeded**: phase 2 marks `ERROR` and exits non-zero → Job fails. Acceptable (output mapping failure = job failure).
- **Sidecar depends on initContainer completing**: guaranteed by k8s initContainer semantics.

## Rollout / migration
- The `wagt` CLI change (subcommand) and the dispatch shell change must ship **together**: a job launched with the new shell but old `wagt` (or vice versa) will misbehave.
- Deploy order: build & push new `wagt` image/binary first (the puller downloads a specific versioned `wagt` URL — bump that version in `command.initContainer.sh`/puller config), then deploy the updated `dispatch_build_and_push.py`. New jobs then use `wagt run`/`wagt finalize`; in-flight jobs continue with the old single-process binary until their pod rotates.
- No persistent-state migration (all shared state is per-pod ephemeral on `emptyDir`).

## Validation
1. `env CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o builds/wagt-<ver>-linux-amd/wagt cmd/main.go` succeeds.


## Open notes
- Decide whether to keep any short `sleep` in the phase-1 shell before exit (optional; not required for correctness).
- `/tmp/wagt.sock` (IPC) stays in `/tmp` — only used within phase 1's container, no cross-container access needed.