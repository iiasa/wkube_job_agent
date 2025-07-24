package services

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

var (
	HTTPClient     *http.Client
	RemoteLogSink  *RemoteLogger
	MultiLogWriter io.Writer
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

// Init initializes shared resources.
func Init() {
	transport := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:        10,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	HTTPClient = &http.Client{
		Transport: &RetryTransport{
			Base:       transport,
			MaxRetries: 5,
			Backoff:    1 * time.Second,
		},
	}

	RemoteLogSink = NewRemoteLogger()

	MultiLogWriter = io.MultiWriter(os.Stdout, RemoteLogSink)

	// os.Stdout = os.NewFile(uintptr(1), "/dev/stdout") // You can skip this if you are on Unix-like system

}
