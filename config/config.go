package config

import (
	"crypto/tls"
	"io"
	"net/http"
	"os"
	"time"
)

var (
	HTTPClient     *http.Client
	RemoteLogSink  *RemoteLogger
	MultiLogWriter io.Writer
)

// Init initializes shared resources.
func Init() {
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
	HTTPClient = &http.Client{
		Transport: transport,
	}

	RemoteLogSink = NewRemoteLogger()

	MultiLogWriter = io.MultiWriter(os.Stdout, RemoteLogSink)

	os.Stdout = os.NewFile(uintptr(1), "/dev/stdout") // You can skip this if you are on Unix-like system

}
