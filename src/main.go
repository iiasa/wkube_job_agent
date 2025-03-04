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
    statusEventData := StatusEventDataType{NewStatus: newStatus}
    eventData := UpdateStatusEventPostData{
        Type:            "STATUS_UPDATE",
        StatusEventData: statusEventData,
    }

    jsonEventData, err := json.Marshal(eventData)
    if err != nil {
        return fmt.Errorf("error marshaling event data json request: %v", err)
    }

    req, err := createRequest("POST", "/v1/ajob-cli/webhook-event/", jsonEventData)
    if err != nil {
        return err
    }

    resp, err := httpClient.Do(req)
    if err != nil {
        return fmt.Errorf("error sending status update HTTP request: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("unexpected status update response status code: %d", resp.StatusCode)
    }

    return nil
}


func sendBatch(lines []byte, logFilename string) error {
    signedURLEndpoint := fmt.Sprintf("/v1/ajob-cli/presigned-log-upload-url/?filename=%s.log", logFilename)
    req, err := createRequest("GET", signedURLEndpoint, nil)
    if err != nil {
        return err
    }

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

    // PUT log data to signed URL
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

    // Register log file
    postData := map[string]interface{}{
        "filename":      signedURLResponse.Filename,
        "app_bucket_id": signedURLResponse.AppBucketId,
    }
    postDataBytes, err := json.Marshal(postData)
    if err != nil {
        return fmt.Errorf("error encoding post data: %v", err)
    }

    postReq, err := createRequest("POST", "/v1/ajob-cli/register-log-file/", postDataBytes)
    if err != nil {
        return err
    }

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
        os.Exit(1)
    }

    return nil
}


func checkHealth() error {
    req, err := createRequest("GET", "/v1/ajob-cli/is-healthy/", nil)
    if err != nil {
        return err
    }

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
        return fmt.Errorf("error decoding health check response: %v", err)
    }

    if !healthCheckResponse.IsHealthy {
        fmt.Println("Process is not healthy. Exiting...")
        os.Exit(1)
    }

    return nil
}

