package main

import (
    "crypto/tls"
    "bufio"
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "os/exec"
    "sync"
    "github.com/google/uuid"
)

func getenvWithDefault(key, fallback string) string {
    if value, ok := os.LookupEnv(key); ok {
        return value
    }
    return fallback
}

// Constants to define chunk size
const chunkSize = 8

// SignedURLResponse represents the JSON response for obtaining a signed URL
type SignedURLResponse struct {
    UploadURL   string `json:"upload_url"`
    Filename    string `json:"filename"`
    AppBucketId int    `json:"app_bucket_id"`
}

func main() {
    if len(os.Args) < 2 {
        fmt.Println("Usage: go run main.go <command>")
        os.Exit(1)
    }

    // Get the command-line argument
    command := os.Args[1]

    // Execute the command with /bin/sh
    cmd := exec.Command("/bin/sh", "-c", command)

    // Create pipes for stdout and stderr
    stdoutPipe, err := cmd.StdoutPipe()
    if err != nil {
        fmt.Println("Error creating stdout pipe:", err)
        os.Exit(1)
    }
    stderrPipe, err := cmd.StderrPipe()
    if err != nil {
        fmt.Println("Error creating stderr pipe:", err)
        os.Exit(1)
    }

    // Start the command
    if err := cmd.Start(); err != nil {
        fmt.Println("Error starting command:", err)
        os.Exit(1)
    }

    // Create scanners for stdout and stderr
    stdoutScanner := bufio.NewScanner(stdoutPipe)
    stderrScanner := bufio.NewScanner(stderrPipe)

    var wg sync.WaitGroup

    // Start goroutines to read stdout and stderr and send chunks over HTTP
    wg.Add(1)
    go func() {
        defer wg.Done()
        sendChunks(stdoutScanner)
    }()
    wg.Add(1)
    go func() {
        defer wg.Done()
        sendChunks(stderrScanner)
    }()

    // Wait for the goroutines to finish
    wg.Wait()

    // Wait for the command to finish
    if err := cmd.Wait(); err != nil {
        fmt.Println("Error waiting for command:", err)
    }
}

func sendChunks(scanner *bufio.Scanner) {
    lines := make([]byte, 0)
    lineCount := 0

    // Iterate through lines and send each batch as a chunk in the HTTP request
    for scanner.Scan() {
        chunk := scanner.Bytes()
       
        lines = append(lines, chunk...)
        lines = append(lines, '\n')
        lineCount++
        fmt.Println("Scanned line:", string(chunk))
        // If reached chunk size, send the batch
        if lineCount == chunkSize {
            sendBatch(lines)
            lines = lines[:0]
            lineCount = 0
        }
    }
    // If there are remaining lines, send the last batch
    if len(lines) > 0 {
        sendBatch(lines)
    }
}

func sendBatch(lines []byte) {

    gatewayServer := getenvWithDefault(
        "GATEWAY_SERVER",
        "https://accelerator-api.iiasa.ac.at/",
    )

    authToken := os.Getenv("JOB_TOKEN")
    if authToken == "" {
        fmt.Println("AUTH_TOKEN environment variable not set")
        os.Exit(1)
    }

    logFilename := uuid.New()

    // Connect to the remote server to get signed URL
    remoteServer := fmt.Sprintf("%sv1/ajob-cli/presigned-upload-url/?filename=%s.log", gatewayServer, logFilename)
    req, err := http.NewRequest("GET", remoteServer, nil)
    if err != nil {
        fmt.Println("Error creating HTTP request:", err)
        os.Exit(1)
    }

    // Set authorization token in request header
    req.Header.Set("X-Authorization", authToken)



    transport := &http.Transport{
        TLSClientConfig: &tls.Config{
            InsecureSkipVerify: true,
        },
    }

    // Create a client to perform the request
    client := &http.Client{
        Transport: transport,
    }

    // First, make a GET request to obtain a signed URL
    resp, err := client.Do(req)
    if err != nil {
        fmt.Println("Error getting signed URL:", err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        fmt.Println("Error: unexpected response code while getting signed URL:", resp.StatusCode)
        return
    }

    var signedURLResponse SignedURLResponse
    if err := json.NewDecoder(resp.Body).Decode(&signedURLResponse); err != nil {
        fmt.Println("Error decoding signed URL response:", err)
        return
    }

    // Now, PUT the chunk to the signed URL
    fmt.Println("Sending batch, size:", len(lines))
    putReq, err := http.NewRequest("PUT", signedURLResponse.UploadURL, bytes.NewReader(lines))
    if err != nil {
        fmt.Println("Error creating PUT request for chunk:", err)
        return
    }
    putResp, err := client.Do(putReq)
    if err != nil {
        fmt.Println("Error sending batch over HTTP:", err)
        return
    }
    defer putResp.Body.Close()

    if putResp.StatusCode != http.StatusOK {
        fmt.Println("Error: unexpected response code while sending chunk:", putResp.StatusCode)
        return
    }

    // Finally, make a POST request to register the chunk with the bucket ID in the request body
    postURL := fmt.Sprintf("%sv1/ajob-cli/register-log-file/", gatewayServer)
    postData := map[string]interface{}{"filename": signedURLResponse.Filename, "app_bucket_id": signedURLResponse.AppBucketId}
    postDataBytes, err := json.Marshal(postData)
    if err != nil {
        fmt.Println("Error encoding post data:", err)
        return
    }
    postReq, err := http.NewRequest("POST", postURL, bytes.NewReader(postDataBytes))
    postReq.Header.Set("X-Authorization", authToken)
    postReq.Header.Set("Content-Type", "application/json")
    postResp, err := client.Do(postReq)
    if err != nil {
        fmt.Println("Error sending POST request to register chunk:", err)
        return
    }
    defer postResp.Body.Close()

    if postResp.StatusCode != http.StatusOK {
        fmt.Println("Error: unexpected response code while registering chunk:", postResp.StatusCode)
        return
    }
}