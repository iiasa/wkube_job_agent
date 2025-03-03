package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"
)

func getenvWithDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// SignedURLResponse represents the JSON response for obtaining a signed URL
type SignedURLResponse struct {
	UploadURL   string `json:"upload_url"`
	Filename    string `json:"filename"`
	AppBucketId int    `json:"app_bucket_id"`
	IsHealthy   bool   `json:"is_healthy"`
}

type HealthCheckResponse struct {
	IsHealthy   bool   `json:"is_healthy"`
}

var (
	httpClient *http.Client
	logCounter int
	counterMu  sync.Mutex // Mutex to protect access to logCounter
)

func init() {
	// Initialize logCounter with the current Unix time
	logCounter = int(time.Now().Unix())

	// Create a transport with connection pooling
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	// Create a client with the transport
	httpClient = &http.Client{
		Transport: transport,
	}
}

func main() {

	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <mode> <command>")
		os.Exit(1)
	}

	mode := os.Args[1]


	if mode == "PRE" {

		if err := updateJobStatus("MAPPING_INPUTS"); err != nil {
			fmt.Println("Error updating status to MAPPING_INPUTS:", err)
			os.Exit(1)
		}

	} else if mode == "MAIN" {

		if err := updateJobStatus("PROCESSING"); err != nil {
			fmt.Println("Error updating status to PROCESSING:", err)
			os.Exit(1)
		}

	} else if mode == "POST" {

		if err := updateJobStatus("MAPPING_OUTPUTS"); err != nil {
			fmt.Println("Error updating status to MAPPING_OUTPUTS:", err)
			os.Exit(1)
		}

	}


	
	
	// Get the command-line argument
	command := os.Args[2]

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
		if err := updateJobStatus("ERROR"); err != nil {
			fmt.Println("Error updating status to ERROR:", err)
			os.Exit(1)
		}
		fmt.Println("Error waiting for command:", err)
		os.Exit(1)
	}


	if mode == "POST"{

		if err := updateJobStatus("DONE"); err != nil {
			fmt.Println("Error updating status to DONE:", err)
			os.Exit(1)
		}

	}
}

func sendChunks(scanner *bufio.Scanner) {
	var lines []byte
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if len(lines) > 0 {
				counterMu.Lock()
				logFilename := fmt.Sprintf("wkube%d", logCounter)
				logCounter++
				counterMu.Unlock()

				if err := sendBatch(lines, logFilename); err != nil {
					fmt.Println("Error sending batch:", err)
					os.Exit(1)
				}
				lines = lines[:0]
			} else {
				if err := checkHealth(); err != nil {
					fmt.Println("Error checking health", err)
					os.Exit(1)
				}
			}
		default:
			if scanner.Scan() {
				chunk := scanner.Bytes()
				lines = append(lines, chunk...)
				lines = append(lines, '\n')
			} else {
				if err := scanner.Err(); err != nil {
					fmt.Println("Scanner error:", err)
				}
				// Send any remaining lines when the scanner is done
				if len(lines) > 0 {
					counterMu.Lock()
					logFilename := fmt.Sprintf("wkube%d", logCounter)
					logCounter++
					counterMu.Unlock()

					if err := sendBatch(lines, logFilename); err != nil {
						fmt.Println("Error sending batch:", err)
						os.Exit(1)
					}
				} else {
					if err := checkHealth(); err != nil {
						fmt.Println("Error checking health", err)
						os.Exit(1)
					}
				}
				return
			}
		}
	}
}

type StatusEventDataType struct {
	NewStatus string `json:"new_status"`
}

// NestedData represents the nested data structure
type UpdateStatusEventPostData struct {
	Type            string              `json:"type"`
	StatusEventData StatusEventDataType `json:"data"`
}

func updateJobStatus(newStatus string) error {
	gatewayServer := getenvWithDefault(
		"ACC_JOB_GATEWAY_SERVER",
		"https://accelerator-api.iiasa.ac.at",
	)

	authToken := os.Getenv("ACC_JOB_TOKEN")
	if authToken == "" {
		fmt.Println("AUTH_TOKEN environment variable not set")
		os.Exit(1)
	}

	statusEventData := StatusEventDataType{
		NewStatus: newStatus,
	}

	eventData := UpdateStatusEventPostData{
		Type:            "STATUS_UPDATE",
		StatusEventData: statusEventData,
	}

	// Marshal the struct into JSON bytes
	jsonEventData, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("error marshaling event data json request: %v", err)
	}

	// Define the URL for the HTTP POST request
	url := fmt.Sprintf("%s/v1/ajob-cli/webhook-event/", gatewayServer)

	statusUpdateReq, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonEventData))
	if err != nil {
		return fmt.Errorf("error creating status update HTTP request: %v", err)
	}

	statusUpdateReq.Header.Set("Content-Type", "application/json")

	// Add custom header
	statusUpdateReq.Header.Set("X-Authorization", authToken)

	// Send HTTP POST request with nested JSON payload
	resp, err := httpClient.Do(statusUpdateReq)
	if err != nil {
		fmt.Println("Error sending status update HTTP request:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	// Check the response status code
	if resp.StatusCode != http.StatusOK {
		fmt.Println("Unexpected status update response status code:", resp.StatusCode)
		os.Exit(1)
	}

	// Optionally, you can process the response body here

	return nil
}

func sendBatch(lines []byte, logFilename string) error {
	gatewayServer := getenvWithDefault(
		"ACC_JOB_GATEWAY_SERVER",
		"https://accelerator-api.iiasa.ac.at",
	)

	authToken := os.Getenv("ACC_JOB_TOKEN")
	if authToken == "" {
		fmt.Println("AUTH_TOKEN environment variable not set")
		os.Exit(1)
	}

	// Connect to the remote server to get signed URL
	remoteServer := fmt.Sprintf("%s/v1/ajob-cli/presigned-log-upload-url/?filename=%s.log", gatewayServer, logFilename)
	req, err := http.NewRequest("GET", remoteServer, nil)
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %v", err)
	}

	// Set authorization token in request header
	req.Header.Set("X-Authorization", authToken)

	// First, make a GET request to obtain a signed URL
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error getting signed URL: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code while getting signed URL: %d", resp.StatusCode)
	}

	var signedURLResponse SignedURLResponse
	if err := json.NewDecoder(resp.Body).Decode(&signedURLResponse); err != nil {
		return fmt.Errorf("error decoding signed URL response: %v", err)
	}

	// Now, PUT the chunk to the signed URL
	putReq, err := http.NewRequest("PUT", signedURLResponse.UploadURL, bytes.NewReader(lines))
	if err != nil {
		return fmt.Errorf("error creating PUT request for chunk: %v", err)
	}
	putResp, err := httpClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("error sending batch over HTTP: %v", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code while sending chunk: %d", putResp.StatusCode)
	}

	// Finally, make a POST request to register the chunk with the bucket ID in the request body
	postURL := fmt.Sprintf("%s/v1/ajob-cli/register-log-file/", gatewayServer)
	postData := map[string]interface{}{"filename": signedURLResponse.Filename, "app_bucket_id": signedURLResponse.AppBucketId}
	postDataBytes, err := json.Marshal(postData)
	if err != nil {
		return fmt.Errorf("error encoding post data: %v", err)
	}
	postReq, err := http.NewRequest("POST", postURL, bytes.NewReader(postDataBytes))
	if err != nil {
		return fmt.Errorf("error creating POST request to register chunk: %v", err)
	}
	postReq.Header.Set("X-Authorization", authToken)
	postReq.Header.Set("Content-Type", "application/json")
	postResp, err := httpClient.Do(postReq)
	if err != nil {
		return fmt.Errorf("error sending POST request to register chunk: %v", err)
	}
	defer postResp.Body.Close()

	if postResp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code while registering chunk: %d", postResp.StatusCode)
	}

	if !signedURLResponse.IsHealthy {
		fmt.Println("Process is not healthy. Exiting...")
		os.Exit(1) // Exit the program with a non-zero status code
	}

	return nil
}

func checkHealth() error {
	gatewayServer := getenvWithDefault(
		"ACC_JOB_GATEWAY_SERVER",
		"https://accelerator-api.iiasa.ac.at",
	)

	authToken := os.Getenv("ACC_JOB_TOKEN")
	if authToken == "" {
		fmt.Println("AUTH_TOKEN environment variable not set")
		os.Exit(1)
	}

	// Connect to the remote server to get signed URL
	remoteServer := fmt.Sprintf("%s/v1/ajob-cli/is-healthy/", gatewayServer)
	req, err := http.NewRequest("GET", remoteServer, nil)
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %v", err)
	}

	// Set authorization token in request header
	req.Header.Set("X-Authorization", authToken)

	// First, make a GET request to obtain a signed URL
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error checking health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code while checking health: %d", resp.StatusCode)
	}

	var healthCheckResponse HealthCheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&healthCheckResponse); err != nil {
		return fmt.Errorf("error decoding signed URL response: %v", err)
	}

	if !healthCheckResponse.IsHealthy {
		fmt.Println("Process is not healthy. Exiting...")
		os.Exit(1) // Exit the program with a non-zero status code
	}

	return nil
}
