package services

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"golang.org/x/net/http2"
)

var (
	HTTPClientWithRetry *http.Client
	HTTPClient          *http.Client
	HTTP2Client         *http.Client
	RemoteLogSink       *RemoteLogger
	MultiLogWriter      io.Writer
	LogFileName         string
)

type RetryTransport struct {
	Base       http.RoundTripper
	MaxRetries int
	Backoff    time.Duration
}

func (r *RetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	base := r.Base
	if base == nil {
		base = http.DefaultTransport
	}

	for attempt := 0; attempt <= r.MaxRetries; attempt++ {
		resp, err = base.RoundTrip(req)
		if err == nil {
			return resp, nil
		}

		// Retry on common network issues
		if !isRetryable(err) {
			return resp, err
		}

		time.Sleep(r.Backoff * (1 << attempt))
	}
	return resp, err
}

func isRetryable(err error) bool {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	return false
}

func Init() {
	// Base transport for regular HTTP/1.1
	transport := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	// HTTP/2 transport setup
	http2Transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	_ = http2.ConfigureTransport(http2Transport)

	// Clients
	HTTPClientWithRetry = &http.Client{
		Transport: &RetryTransport{
			Base:       transport,
			MaxRetries: 2,
			Backoff:    1 * time.Second,
		},
	}

	HTTPClient = &http.Client{
		Transport: transport,
	}

	HTTP2Client = &http.Client{
		Transport: &RetryTransport{
			Base:       http2Transport,
			MaxRetries: 2,
			Backoff:    1 * time.Second,
		},
	}

	podID := os.Getenv("POD_ID")
	if podID == "" {
		podID = "unknown" // fallback if POD_ID is not set
	}

	LogFileName = fmt.Sprintf("job-%s.log", podID)

	logFile, err := os.OpenFile("/tmp/job.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic("failed to open log file: " + err.Error())
	}

	RemoteLogSink = NewRemoteLogger()
	MultiLogWriter = io.MultiWriter(os.Stdout, RemoteLogSink, logFile)
}
