package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func getenvWithDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func createRequest(method, endpoint string, body []byte) (*http.Request, error) {
	gatewayServer := getenvWithDefault(
		"ACC_JOB_GATEWAY_SERVER",
		"https://accelerator-api.iiasa.ac.at",
	)

	authToken := os.Getenv("ACC_JOB_TOKEN")
	if authToken == "" {
		return nil, fmt.Errorf("AUTH_TOKEN environment variable not set")
	}

	url := fmt.Sprintf("%s%s", gatewayServer, endpoint)
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

// getMultipartPutCreateSignedURL retrieves a signed URL for creating a multipart upload.
func getMultipartPutCreateSignedURL(appBucketID, objectName, uploadID string, partNumber int) (map[string]interface{}, error) {
	endpoint := fmt.Sprintf("/put-create-signed-url?app_bucket_id=%s&object_name=%s&upload_id=%s&part_number=%d", appBucketID, objectName, uploadID, partNumber)
	req, err := createRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// getPutCreateMultipartUploadID retrieves a multipart upload ID for creating a new upload.
func getPutCreateMultipartUploadID(filename string) (string, string, string, error) {
	endpoint := fmt.Sprintf("/create-multipart-upload-id/%s", filename)
	req, err := createRequest("GET", endpoint, nil)
	if err != nil {
		return "", "", "", err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", "", "", err
	}
	defer resp.Body.Close()

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", "", err
	}

	return result["upload_id"], result["app_bucket_id"], result["uniqified_filename"], nil
}

// completeJobMultipartUpload completes a multipart upload for a job.
func completeJobMultipartUpload(appBucketID, filename, uploadID string, parts [][]string, isLogFile bool) (map[string]interface{}, error) {
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

	req, err := createRequest("PUT", "/complete-create-multipart-upload", body)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// abortCreateMultipartUpload aborts a multipart upload for creation.
func abortCreateMultipartUpload(appBucketID, filename, uploadID string) (map[string]interface{}, error) {
	payload := map[string]interface{}{
		"app_bucket_id": appBucketID,
		"filename":      filename,
		"upload_id":     uploadID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := createRequest("PUT", "/abort-create-multipart-upload", body)
	if err != nil {
		return nil, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func enumerateFilesByPrefix(prefix string) ([]string, error) {
	// Extract the project slug from the prefix (assuming the prefix starts with the project slug)
	projectSlug := strings.Split(prefix, "/")[0]

	// Base64 encode the prefix
	b64EncodedPrefix := base64.StdEncoding.EncodeToString([]byte(prefix))

	// Construct the endpoint
	endpoint := fmt.Sprintf("/%s/enumerate-all-files/%s", projectSlug, b64EncodedPrefix)

	// Create the HTTP request
	req, err := createRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	// Send the request
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

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

// addFilestreamAsJobOutput uploads a file stream as a job output using multipart upload.
func addFilestreamAsJobOutput(filename string, fileStream io.Reader, isLogFile bool) (map[string]interface{}, error) {
	partSize := 50 * 1024 * 1024 // 50 MB

	var uploadID, appBucketID, uniqifiedFilename string
	var oneByte []byte
	stop := false
	partNumber := 0
	parts := [][]byte{} // Each part is a byte slice
	uploadedSize := 0
	var putPresignedURL string

	defer func() {
		// Cancel the upload if an error occurs
		if uploadID != "" {
			_, _ = abortCreateMultipartUpload(appBucketID, uniqifiedFilename, uploadID)
		}
	}()

	for !stop {
		partNumber++
		partData, err := readPartData(fileStream, partSize+1, oneByte)
		if err != nil {
			return nil, fmt.Errorf("error reading part data: %v", err)
		}

		// If partData size is less than or equal to partSize, this is the last part.
		if len(partData) <= partSize {
			stop = true
		} else {
			oneByte = partData[len(partData)-1:]
			partData = partData[:len(partData)-1]
		}

		uploadedSize += len(partData)

		// Get the upload ID if not already obtained
		if uploadID == "" {
			uploadID, appBucketID, uniqifiedFilename, err = getPutCreateMultipartUploadID(filename)
			if err != nil {
				return nil, fmt.Errorf("error getting upload ID: %v", err)
			}
		}

		// Get the signed URL for uploading the part
		signedURLResult, err := getMultipartPutCreateSignedURL(appBucketID, uniqifiedFilename, uploadID, partNumber)
		if err != nil {
			return nil, fmt.Errorf("error getting signed URL: %v", err)
		}
		putPresignedURL = signedURLResult["url"].(string) // Assuming the signed URL is in the "url" field

		// Upload the part
		req, err := http.NewRequest("PUT", putPresignedURL, bytes.NewReader(partData))
		if err != nil {
			return nil, fmt.Errorf("error creating part upload request: %v", err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("error uploading part: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("part upload failed with status: %s", resp.Status)
		}

		// Extract the ETag from the response headers
		etag := resp.Header.Get("ETag")
		if etag == "" {
			return nil, errors.New("ETag not found in part upload response")
		}
		etag = strings.Trim(etag, `"`) // Remove quotes from ETag

		// Store the part data (as bytes) and ETag
		parts = append(parts, partData)
	}

	// Complete the multipart upload
	createdBucketObjectID, err := completeJobMultipartUpload(appBucketID, uniqifiedFilename, uploadID, parts, isLogFile)
	if err != nil {
		return nil, fmt.Errorf("error completing multipart upload: %v", err)
	}

	return createdBucketObjectID, nil
}

func uploadFile(filePath string) error {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Extract the filename from the file path
	filename := filePath
	if strings.Contains(filePath, "/") {
		filename = filePath[strings.LastIndex(filePath, "/")+1:]
	}

	// Upload the file
	result, err := addFilestreamAsJobOutput(filename, file, false)
	if err != nil {
		return fmt.Errorf("error uploading file: %v", err)
	}

	fmt.Println("Upload successful. Bucket Object ID:", result)
	return nil
}

func getFileURLFromRepo(filename string) (string, error) {
	// Extract the project slug from the filename
	projectSlug := strings.Split(filename, "/")[0]

	// Construct the endpoint
	endpoint := fmt.Sprintf("/%s/get-file-download-url/?filename=%s", projectSlug, filename)

	// Create the HTTP request
	req, err := createRequest("GET", endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	// Send the request
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// Decode the response
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("error decoding response: %v", err)
	}

	// Extract the download URL
	downloadURL, ok := result["url"].(string)
	if !ok {
		return "", errors.New("download URL not found in response")
	}

	return downloadURL, nil
}

// downloadFileFromURL downloads a file from the given URL and saves it to the specified path.
func downloadFileFromURL(url, outputPath string) error {
	// Create the output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer outputFile.Close()

	// Send a GET request to the download URL
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error downloading file: %v", err)
	}
	defer resp.Body.Close()

	// Check if the response status is OK
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed with status: %s", resp.Status)
	}

	// Copy the response body to the output file
	_, err = io.Copy(outputFile, resp.Body)
	if err != nil {
		return fmt.Errorf("error saving file: %v", err)
	}

	return nil
}

// downloadFileFromRepo downloads a file from the repository to the specified path.
func downloadFileFromRepo(filename, outputPath string) error {
	// Get the download URL for the file
	downloadURL, err := getFileURLFromRepo(filename)
	if err != nil {
		return fmt.Errorf("error getting download URL: %v", err)
	}

	// Download the file from the URL
	if err := downloadFileFromURL(downloadURL, outputPath); err != nil {
		return fmt.Errorf("error downloading file: %v", err)
	}

	fmt.Printf("File downloaded successfully to %s\n", outputPath)
	return nil
}
