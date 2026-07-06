## WKUBE Job Agent (IIASA Accelerator)

This agent injects into a wkube job, performs data mapping on startup and finalization, and monitors its health. Inspired by HKube/Argo Workflows. Injects as a lightweight I/O-bound trait (Goroutine) in the main container. The agent becomes like the init system of the container that runs the job and wraps the command being launched.

## Usage
`go run main.go "bash command"`

## Build
`env CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o builds/wagt-v1.3.1-linux-amd/wagt cmd/main.go`

## Input and Output Mappings

The agent handles file/directory mappings between mounted storage (`/mnt/tmp` or `/mnt/wdrv`), container local storage, and remote Accelerator repositories (`acc://`).

### 1. Syntax

Mappings are defined as a semicolon-separated list of `source:destination` strings in the environment variables:
* `input_mappings`: Sets up files/directories before the job starts.
* `output_mappings`: Sets up output directories and uploads/saves files after the job completes.

### 2. Standard Mappings (No Wildcards)

* **Mapping a single file to a file**:
  When `source` and `destination` are both files. Creates a symlink directly at the destination pointing to the source.
  * Example: `/mnt/tmp/data/config.yaml:/app/config.yaml`
* **Mapping a single file into a directory**:
  If the destination ends with a trailing slash `/`, the file will be mapped inside the destination directory with its original filename.
  * Example: `/mnt/tmp/data/config.yaml:/app/config/` (creates `/app/config/config.yaml`)
* **Mapping a directory**:
  If the source is a directory, the destination **must** end with a trailing slash `/`. The destination directory will point to the source directory.
  * Example: `/mnt/tmp/dataset/:/app/dataset/`

### 3. Wildcard / Glob Mappings

If the source path contains wildcard characters (`*`, `?`, `[]`), the agent will expand the glob matches under `/mnt/tmp` or `/mnt/wdrv`.

* **Rule**: The destination path **must** end with a trailing slash `/` because it serves as the target directory.
* **Behavior**: Each matched file or directory is symlinked inside the destination directory with its base name.
* **Example (Importing specific files)**:
  `input_mappings="/mnt/tmp/raw_data/*.csv:/app/data/"`
  If `/mnt/tmp/raw_data` contains `a.csv`, `b.csv`, and `other.txt`, it will create symlinks:
  * `/app/data/a.csv` -> `/mnt/tmp/raw_data/a.csv`
  * `/app/data/b.csv` -> `/mnt/tmp/raw_data/b.csv`
  *(Note: `other.txt` is not mapped)*
* **Example (Matching subdirectories)**:
  `input_mappings="/mnt/tmp/runs/run_2026_*/:/app/runs/"`
  Creates symlinks inside `/app/runs/` for each matched subdirectory (e.g. `/app/runs/run_2026_07_06`).
