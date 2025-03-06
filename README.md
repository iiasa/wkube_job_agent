## WKUBE Job Agent (IIASA Accelerator)

This agent injects into a wkube job, performs data mapping on startup and finalization, and monitors its health. 

## Usage
`go run main.go "bash command"`

## Build
`env GOOS=linux GOARCH=amd64 go build -o wagt-v0.5.4-linux-amd/wagt main.go`
