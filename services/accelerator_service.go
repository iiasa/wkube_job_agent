package services

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	tokenMutex          sync.Mutex
	currentRefreshToken string
	cachedAccessToken   string
	cachedExpiresAt     time.Time
)

// GetAccessToken retrieves a valid access token. If no valid access token is cached,
// it exchanges ACC_JOB_API_REFRESH_TOKEN (with token rotation) and caches it.
func GetAccessToken() (string, int64, error) {
	tokenMutex.Lock()
	defer tokenMutex.Unlock()

	now := time.Now()
	// Reuse cached access token if it has at least 5 minutes remaining
	if cachedAccessToken != "" && now.Before(cachedExpiresAt.Add(-5*time.Minute)) {
		return cachedAccessToken, cachedExpiresAt.Unix(), nil
	}

	if currentRefreshToken == "" {
		currentRefreshToken = os.Getenv("ACC_JOB_API_REFRESH_TOKEN")
	}

	if currentRefreshToken == "" {
		return "", 0, fmt.Errorf("ACC_JOB_API_REFRESH_TOKEN environment variable is not set")
	}

	gatewayServer := getenvWithDefault(
		"ACC_JOB_GATEWAY_SERVER",
		"https://accelerator.iiasa.ac.at",
	)
	refreshURL := fmt.Sprintf("%s/api/v1/oauth/device/access-token/", strings.TrimRight(gatewayServer, "/"))

	type RefreshPayload struct {
		RefreshToken string `json:"refresh_token"`
	}
	type RefreshResponse struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}

	payloadBytes, err := json.Marshal(RefreshPayload{RefreshToken: currentRefreshToken})
	if err != nil {
		return "", 0, fmt.Errorf("failed to marshal refresh payload: %w", err)
	}

	req, err := http.NewRequest("POST", refreshURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return "", 0, fmt.Errorf("failed to create refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Use HTTPClientWithRetry since it handles connection errors/retries and is defined in config.go
	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("failed to send refresh token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(resp.Body)
		return "", 0, fmt.Errorf("refresh token request returned status %d: %s", resp.StatusCode, string(errBody))
	}

	var respData RefreshResponse
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return "", 0, fmt.Errorf("failed to decode refresh response: %w", err)
	}

	if respData.AccessToken == "" {
		return "", 0, fmt.Errorf("refresh response did not contain access token")
	}

	// Update the refresh token for the next rotation
	currentRefreshToken = respData.RefreshToken
	cachedAccessToken = respData.AccessToken
	// Assume 1 hour access token lifetime, expire slightly earlier (50 minutes)
	cachedExpiresAt = now.Add(50 * time.Minute)

	return cachedAccessToken, cachedExpiresAt.Unix(), nil
}

func getenvWithDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func CreateRequest(method, endpoint string, body []byte) (*http.Request, error) {
	gatewayServer := getenvWithDefault(
		"ACC_JOB_GATEWAY_SERVER",
		"https://accelerator.iiasa.ac.at",
	)

	authToken, _, err := GetAccessToken()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve access token: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/ajob-cli%s", gatewayServer, endpoint)
	var req *http.Request

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
	IsHealthy   bool   `json:"is_healthy"`
}

type HealthCheckResponse struct {
	IsHealthy bool `json:"is_healthy"`
}

func SendWebhookEvent(eventType string, payload interface{}) error {
	// Wrap payload with event type
	eventData := map[string]interface{}{
		"type": eventType,
		"data": payload,
	}

	jsonEventData, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("error marshaling webhook event JSON: %v", err)
	}

	endpoint := "/webhook-event/"

	req, err := CreateRequest("POST", endpoint, jsonEventData)
	if err != nil {
		return fmt.Errorf("error creating HTTP request: %v", err)
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return fmt.Errorf("error sending webhook HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return fmt.Errorf("POST %s returned non-OK status: %v", endpoint, err)
	}

	return nil
}

func UpdateJobStatus(newStatus string) error {
	payload := StatusEventDataType{NewStatus: newStatus}
	return SendWebhookEvent("STATUS_UPDATE", payload)
}

func ReportNodeName() error {
	type NodeNameReportDataType struct {
		Name string `json:"name"`
	}
	nodeName := os.Getenv("CLUSTER_NODE_NAME")
	if nodeName == "" {
		fmt.Fprintf(MultiLogWriter, "CLUSTER_NODE_NAME environment variable not found")
		return nil
	}
	payload := NodeNameReportDataType{Name: nodeName}

	return SendWebhookEvent("NODE_NAME", payload)
}

func SendBatch(lines []byte, logFilename string, cancel context.CancelFunc) error {
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

	if !signedURLResponse.IsHealthy {

		fmt.Fprintf(MultiLogWriter, "Health check failed: job is not healthy\n")

		if cancel != nil {
			cancel()
		}
	}

	return nil
}

func CheckHealth(cancel context.CancelFunc) (bool, error) {
	endpoint := "/is-healthy/"
	req, err := CreateRequest("GET", endpoint, nil)
	if err != nil {
		return false, err
	}

	resp, err := HTTPClientWithRetry.Do(req)
	if err != nil {
		return false, fmt.Errorf("error checking health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := HandleHTTPError(resp)
		return false, fmt.Errorf("GET %s returned not okay status %v", endpoint, err)
	}

	var healthCheckResponse HealthCheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&healthCheckResponse); err != nil {
		return false, fmt.Errorf("error decoding health check response: %v", err)
	}

	if !healthCheckResponse.IsHealthy {
		if cancel != nil {
			cancel()
		}
	}

	return healthCheckResponse.IsHealthy, nil
}

type RegisterValidationPayload struct {
	ValidatedFilename             string          `json:"validated_filename"`
	DatasetTemplateID             int             `json:"dataset_template_id"`
	ValidatedMetadata             json.RawMessage `json:"validated_metadata"`
	ValidationSupportingFilenames []string        `json:"validation_supporting_filenames"`
}

func RegisterValidationWithFilename(payload *RegisterValidationPayload) (bool, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal payload: %w", err)
	}

	httpReq, err := CreateRequest("PUT", "/register-validation-with-filename/", payloadBytes)
	if err != nil {
		return false, fmt.Errorf("failed to create gateway request: %w", err)
	}

	resp, err := HTTPClientWithRetry.Do(httpReq)
	if err != nil {
		return false, fmt.Errorf("gateway request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("gateway returned status %d: %s", resp.StatusCode, string(respBytes))
	}

	var result bool
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode gateway response: %w", err)
	}

	return result, nil
}

