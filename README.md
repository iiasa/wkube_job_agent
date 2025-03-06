## WKUBE Job Agent (IIASA Accelerator)

This agent injects into a wkube job, performs data mapping on startup and finalization, and monitors its health. Inspired by HKube/Argo Workflows. Injects as a lightweight I/O-bound trait (Goroutine) in the main container. The agent becomes like the init system of the container that runs the job and wraps the command being launched.

## Usage
`go run main.go "bash command"`

## Build
`env GOOS=linux GOARCH=amd64 go build -o wagt-v0.5.4-linux-amd/wagt main.go`
