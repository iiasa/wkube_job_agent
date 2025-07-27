package services

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func getenvWithDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func CreateRequest(method, endpoint string, body []byte) (*http.Request, error) {
	gatewayServer := getenvWithDefault(
		"ACC_JOB_GATEWAY_SERVER",
		"https://accelerator-api.iiasa.ac.at",
	)

	authToken := os.Getenv("ACC_JOB_TOKEN")
	if authToken == "" {
		return nil, fmt.Errorf("AUTH_TOKEN environment variable not set")
	}

	url := fmt.Sprintf("%s/v1/ajob-cli%s", gatewayServer, endpoint)
	var req *http.Request
	var err error

	if body != nil {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(body))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}

	if err != nil {
		return nil, fmt.Errorf("error creating HTTP request: %v", err)
	}

	req.Header.Set("X-Authorization", authToken)
	req.Header.Set("Content-Type", "application/json")

	return req, nil
}

func HandleHTTPError(resp *http.Response) error {
	if resp == nil {
		return fmt.Errorf("response is nil with status")
	}

	var body string
	if resp.Body != nil {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body (status %d): %w", resp.StatusCode, err)
		}
		body = string(b)
	}

	return fmt.Errorf("received status %d, response: %s", resp.StatusCode, body)
}

func getMultipartPutCreateSignedURL(appBucketID int, objectName, uploadID string, partNumber int) (*string, error) {
	endpoint := fmt.Sprintf("/put-create-signed-url?app_bucket_id=%d&object_name=%s&upload_id=%s&part_number=%d", appBucketID, objectName, uploadID, partNumber)
	req, err := CreateRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return nil, fmt.Errorf("GET %s returned not okay status %v", endpoint, err)
	}

	var result string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

type MultipartUploadIDCreateResponse struct {
	UploadID           string `json:"upload_id"`
	AppBucketID        int    `json:"app_bucket_id"`
	UniquifiedFilename string `json:"uniqified_filename"`
}

// getPutCreateMultipartUploadID retrieves a multipart upload ID for creating a new upload.
func getPutCreateMultipartUploadID(filename string) (*MultipartUploadIDCreateResponse, error) {
	encodedFilename := url.PathEscape(filename)
	endpoint := fmt.Sprintf("/multipart-upload-id?filename=%s", encodedFilename)
	req, err := CreateRequest("GET", endpoint, nil)

	if err != nil {
		return nil, err
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return nil, fmt.Errorf("GET %s returned not okay status %v", endpoint, err)
	}

	var result MultipartUploadIDCreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// completeJobMultipartUpload completes a multipart upload for a job.
func completeJobMultipartUpload(appBucketID int, filename, uploadID string, parts [][]string, isLogFile bool) (*int, error) {
	partsJSON, err := json.Marshal(parts)
	if err != nil {
		return nil, err
	}
	partsBase64 := base64.StdEncoding.EncodeToString(partsJSON)

	payload := map[string]interface{}{
		"app_bucket_id": appBucketID,
		"filename":      filename,
		"upload_id":     uploadID,
		"parts":         partsBase64,
		"is_log_file":   isLogFile,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	endpoint := "/complete-create-multipart-upload"

	req, err := CreateRequest("PUT", endpoint, body)
	if err != nil {
		return nil, err
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return nil, fmt.Errorf("PUT %s returned not okay status %v", endpoint, err)
	}

	var result *int
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// abortCreateMultipartUpload aborts a multipart upload for creation.
func abortCreateMultipartUpload(appBucketID int, filename, uploadID string) (*bool, error) {
	payload := map[string]interface{}{
		"app_bucket_id": appBucketID,
		"filename":      filename,
		"upload_id":     uploadID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	endpoint := "/abort-create-multipart-upload"

	req, err := CreateRequest("PUT", endpoint, body)
	if err != nil {
		return nil, err
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return nil, fmt.Errorf("PUT %s returned not okay status %v", endpoint, err)
	}

	var result *bool
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func EnumerateFilesByPrefix(prefix string) ([]string, error) {
	// Extract the project slug from the prefix (assuming the prefix starts with the project slug)
	projectSlug := strings.Split(prefix, "/")[0]

	// Base64 encode the prefix
	b64EncodedPrefix := base64.StdEncoding.EncodeToString([]byte(prefix))

	// Construct the endpoint
	endpoint := fmt.Sprintf("/%s/enumerate-all-files/%s", projectSlug, b64EncodedPrefix)

	// Create the HTTP request
	req, err := CreateRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	// Send the request
	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return nil, fmt.Errorf("GET %s returned not okay status %v", endpoint, err)
	}

	// Decode the response
	var result []string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// readPartData reads part data of a given size from a stream.
func readPartData(stream io.Reader, size int, partData []byte) ([]byte, error) {
	size -= len(partData)
	buf := make([]byte, size)
	for size > 0 {
		n, err := stream.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			break // EOF reached
		}
		partData = append(partData, buf[:n]...)
		size -= n
	}
	return partData, nil
}

func addFilestreamAsJobOutput(filename string, fileStream io.Reader, isLogFile bool) (result *int, err error) {
	partSize := 100 * 1024 * 1024
	var uploadIDDetailsResponse = &MultipartUploadIDCreateResponse{}
	var oneByte []byte
	stop := false
	partNumber := 0
	var uploadedSize int
	var putPresignedURL string

	var parts [][]string
	var mutex sync.Mutex
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 5) // Limit to 5 concurrent uploads
	errChan := make(chan error, 1)      // Capture any errors

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancellation on function exit

	for !stop {
		partNumber++
		partData, err := readPartData(fileStream, partSize+1, oneByte)
		if err != nil {
			cancel() // Cancel all Goroutines
			return nil, fmt.Errorf("error reading part data: %v", err)
		}

		if len(partData) <= partSize {
			stop = true
		} else {
			oneByte = partData[len(partData)-1:]
			partData = partData[:len(partData)-1]
		}

		uploadedSize += len(partData)

		if uploadIDDetailsResponse.UploadID == "" {
			uploadIDDetailsResponse, err = getPutCreateMultipartUploadID(filename)
			if err != nil {
				cancel()
				return nil, fmt.Errorf("error getting upload ID: %v", err)
			}
		}

		signedURLResult, err := getMultipartPutCreateSignedURL(uploadIDDetailsResponse.AppBucketID,
			uploadIDDetailsResponse.UniquifiedFilename,
			uploadIDDetailsResponse.UploadID,
			partNumber)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("error getting signed URL: %v", err)
		}
		putPresignedURL = *signedURLResult

		wg.Add(1)
		semaphore <- struct{}{} // Acquire slot

		go func(partNumber int, partData []byte, putPresignedURL string) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release slot

			select {
			case <-ctx.Done():
				return
			default:
			}

			req, err := http.NewRequest("PUT", putPresignedURL, bytes.NewReader(partData))
			if err != nil {
				errChan <- fmt.Errorf("error creating part upload request: %v", err)
				cancel()
				return
			}
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := HTTPClientWithRetry.Do(req)
			if err != nil {
				errChan <- fmt.Errorf("error uploading part: %v", err)
				cancel()
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errChan <- fmt.Errorf("part upload failed with status: %s", resp.Status)
				cancel()
				return
			}

			etag := resp.Header.Get("ETag")
			if etag == "" {
				errChan <- errors.New("ETag not found in part upload response")
				cancel()
				return
			}
			etag = strings.Trim(etag, `"`)

			mutex.Lock()
			parts = append(parts, []string{fmt.Sprintf("%d", partNumber), etag})
			mutex.Unlock()

		}(partNumber, partData, putPresignedURL)
	}

	wg.Wait()

	select {
	case err = <-errChan:
		return nil, fmt.Errorf("error uploading part of multipart upload: %w", err)
	default:
	}

	// Sort the parts array by partNumber (converted to integer) before calling completeJobMultipartUpload
	sort.Slice(parts, func(i, j int) bool {
		partNumberI, _ := strconv.Atoi(parts[i][0])
		partNumberJ, _ := strconv.Atoi(parts[j][0])
		return partNumberI < partNumberJ
	})

	// Complete the upload only if everything is successful
	result, err = completeJobMultipartUpload(uploadIDDetailsResponse.AppBucketID,
		uploadIDDetailsResponse.UniquifiedFilename,
		uploadIDDetailsResponse.UploadID, parts, isLogFile)
	if err != nil {
		return nil, fmt.Errorf("error completing multipart upload: %v", err)
	}

	if err != nil && uploadIDDetailsResponse.UploadID != "" {
		_, err = abortCreateMultipartUpload(uploadIDDetailsResponse.AppBucketID,
			uploadIDDetailsResponse.UniquifiedFilename,
			uploadIDDetailsResponse.UploadID)
		return nil, fmt.Errorf("error aborting upload: %v", err)
	}

	return result, nil
}

func UploadFile(localPath string, remotePath string) error {

	fmt.Fprintf(MultiLogWriter, "Uploading file: %s to remote job output folder at %s \n", localPath, remotePath)

	// Open the files
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Upload the file with the given remote path
	result, err := addFilestreamAsJobOutput(remotePath, file, false)
	if err != nil {
		return fmt.Errorf("error uploading file: %v", err)
	}

	fmt.Fprintf(MultiLogWriter, "Upload successful. Bucket Object ID: %d \n", *result)
	return nil
}

func getFileURLFromRepo(filename string) (string, error) {
	// Extract the project slug from the filename
	projectSlug := strings.Split(filename, "/")[0]

	// Construct the endpoint
	endpoint := fmt.Sprintf("/%s/get-file-download-url/?filename=%s", projectSlug, filename)

	// Create the HTTP request
	req, err := CreateRequest("GET", endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	// Send the request
	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return "", fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return "", fmt.Errorf("GET %s returned not okay status %v", endpoint, err)
	}

	// Decode the response
	var result string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("error decoding response: %v", err)
	}

	return result, nil
}

// downloadFileFromURL downloads a file from the given URL and saves it to the specified path.
func downloadFileFromURL(url, outputPath string) error {
	// Create the output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer outputFile.Close()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("error creating request %v", err)

	}

	// Send a GET request to the download URL
	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return fmt.Errorf("error downloading file: %v", err)
	}
	defer resp.Body.Close()

	// Check if the response status is OK
	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return fmt.Errorf("GET %s returned not okay status %v", url, err)
	}

	// Copy the response body to the output file
	_, err = io.Copy(outputFile, resp.Body)
	if err != nil {
		return fmt.Errorf("error saving file: %v", err)
	}

	return nil
}

// downloadFileFromRepo downloads a file from the repository to the specified path.
func DownloadFileFromRepo(filename, outputPath string) error {
	// Get the download URL for the file
	downloadURL, err := getFileURLFromRepo(filename)
	if err != nil {
		return fmt.Errorf("error getting download URL: %v", err)
	}

	// Download the file from the URL
	if err := downloadFileFromURL(downloadURL, outputPath); err != nil {
		return fmt.Errorf("error downloading file: %v", err)
	}

	fmt.Fprintf(MultiLogWriter, "File downloaded successfully to %s\n", outputPath)
	return nil
}

type StatusEventDataType struct {
	NewStatus string `json:"new_status"`
}

// NestedData represents the nested data structure
type UpdateStatusEventPostData struct {
	Type            string              `json:"type"`
	StatusEventData StatusEventDataType `json:"data"`
}

type SignedURLResponse struct {
	UploadURL   string `json:"upload_url"`
	Filename    string `json:"filename"`
	AppBucketId int    `json:"app_bucket_id"`
	IsHealthy   bool   `json:"is_healthy"`
}

type HealthCheckResponse struct {
	IsHealthy bool `json:"is_healthy"`
}

func UpdateJobStatus(newStatus string) error {
	statusEventData := StatusEventDataType{NewStatus: newStatus}
	eventData := UpdateStatusEventPostData{
		Type:            "STATUS_UPDATE",
		StatusEventData: statusEventData,
	}

	jsonEventData, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("error marshaling event data json request: %v", err)
	}

	endpoint := "/webhook-event/"

	req, err := CreateRequest("POST", endpoint, jsonEventData)
	if err != nil {
		return err
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return fmt.Errorf("error sending status update HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return fmt.Errorf("POST %s returned not okay status %v", endpoint, err)
	}

	return nil
}

func SendBatch(lines []byte, logFilename string) error {
	signedURLEndpoint := fmt.Sprintf("/presigned-log-upload-url/?filename=%s.log", logFilename)
	req, err := CreateRequest("GET", signedURLEndpoint, nil)
	if err != nil {
		return err
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return fmt.Errorf("error getting signed URL: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return fmt.Errorf("GET %s returned not okay status %v", signedURLEndpoint, err)
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

	putResp, err := HTTPClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("error sending batch over HTTP: %v", err)
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		err := HandleHTTPError(putResp)
		return fmt.Errorf("PUT %s returned not okay status %v", signedURLResponse.UploadURL, err)
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

	registerEndpoint := "/register-log-file/"

	postReq, err := CreateRequest("POST", registerEndpoint, postDataBytes)
	if err != nil {
		return err
	}

	postResp, err := HTTPClientWithRetry.Do(postReq)
	if err != nil {
		return fmt.Errorf("error sending POST request to register chunk: %v", err)
	}
	defer postResp.Body.Close()

	if postResp.StatusCode != http.StatusOK {
		err := HandleHTTPError(postResp)
		return fmt.Errorf("POST %s returned not okay status %v", registerEndpoint, err)
	}

	if !signedURLResponse.IsHealthy {
		if err := PostProcessMappings(); err != nil {
			fmt.Fprintf(MultiLogWriter, "error in post-process-mappings upon bad health of job: %v", err)
		}

		if err := VerboseResourceReport(); err != nil {
			fmt.Fprintf(MultiLogWriter, "Error generating resource report: %v\n", err)
		}

		if err := UploadFile("/tmp/job.log", LogFileName); err != nil {
			fmt.Fprintf(MultiLogWriter, "error uploading job log: %v", err)
		}
		RemoteLogSink.Close()

		os.Exit(1)
	}

	return nil
}

func CheckHealth() error {
	endpoint := "/is-healthy/"
	req, err := CreateRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return fmt.Errorf("error checking health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return fmt.Errorf("GET %s returned not okay status %v", endpoint, err)
	}

	var healthCheckResponse HealthCheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&healthCheckResponse); err != nil {
		return fmt.Errorf("error decoding health check response: %v", err)
	}

	if !healthCheckResponse.IsHealthy {
		if err := PostProcessMappings(); err != nil {
			fmt.Fprintf(MultiLogWriter, "error in post-process-mappings upon bad health of job: %v", err)
		}
		if err := VerboseResourceReport(); err != nil {
			fmt.Fprintf(MultiLogWriter, "Error generating resource report: %v\n", err)
		}

		if err := UploadFile("/tmp/job.log", LogFileName); err != nil {
			fmt.Fprintf(MultiLogWriter, "error uploading job log: %v", err)
		}
		RemoteLogSink.Close()
		os.Exit(1)
	}

	return nil
}
