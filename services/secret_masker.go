package services

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	secretMask = "***"
	// Shorter values would mask ordinary words in the log.
	minMaskedSecretLength = 4
)

// SecretMasker replaces job secret values with *** before log bytes reach stdout, the remote
// sink or the local log file. A value split across two writes is caught by holding back a
// tail that could still be the start of a secret until the next write (or Flush).
type SecretMasker struct {
	mu      sync.Mutex
	out     io.Writer
	secrets [][]byte // longest first, so a secret containing another is masked whole
	pending []byte
}

func NewSecretMasker(out io.Writer, values []string) *SecretMasker {
	seen := map[string]bool{}
	var secrets [][]byte
	for _, value := range values {
		for _, variant := range secretVariants(value) {
			if len(variant) < minMaskedSecretLength || seen[variant] {
				continue
			}
			seen[variant] = true
			secrets = append(secrets, []byte(variant))
		}
	}
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	return &SecretMasker{out: out, secrets: secrets}
}

// secretVariants are the forms a value commonly takes when a job prints it.
func secretVariants(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return []string{
		value,
		base64.StdEncoding.EncodeToString([]byte(value)),
		base64.RawStdEncoding.EncodeToString([]byte(value)),
		base64.URLEncoding.EncodeToString([]byte(value)),
		base64.RawURLEncoding.EncodeToString([]byte(value)),
		url.QueryEscape(value),
		url.PathEscape(value),
	}
}

func (m *SecretMasker) Write(p []byte) (int, error) {
	if len(m.secrets) == 0 {
		return m.out.Write(p)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	data := append(m.pending, p...)
	for _, secret := range m.secrets {
		data = bytes.ReplaceAll(data, secret, []byte(secretMask))
	}

	hold := m.partialSecretSuffix(data)
	m.pending = append([]byte(nil), data[len(data)-hold:]...)

	if emit := data[:len(data)-hold]; len(emit) > 0 {
		if _, err := m.out.Write(emit); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}

// partialSecretSuffix is the length of the longest tail of data that is a proper prefix of a secret.
func (m *SecretMasker) partialSecretSuffix(data []byte) int {
	longest := 0
	for _, secret := range m.secrets {
		max := len(secret) - 1
		if max > len(data) {
			max = len(data)
		}
		for n := max; n > longest; n-- {
			if bytes.Equal(data[len(data)-n:], secret[:n]) {
				longest = n
				break
			}
		}
	}
	return longest
}

// Flush writes out a held-back tail; it can no longer complete a secret.
func (m *SecretMasker) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pending) > 0 {
		m.out.Write(m.pending)
		m.pending = nil
	}
}

// FlushLogMasker releases any held-back log tail; call before flushing or uploading logs.
func FlushLogMasker() {
	if LogMasker != nil {
		LogMasker.Flush()
	}
}

// LoadSecretsToMask reads the values the scheduler marked for masking: env vars named in
// WAGT_MASK_ENV and the files under WAGT_MASK_FILES_DIR.
func LoadSecretsToMask() []string {
	var values []string
	for _, name := range strings.Split(os.Getenv("WAGT_MASK_ENV"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			values = append(values, os.Getenv(name))
		}
	}

	if dir := os.Getenv("WAGT_MASK_FILES_DIR"); dir != "" {
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			// Secret volumes link each file through a ..data directory; skip the internals.
			if strings.HasPrefix(entry.Name(), "..") {
				continue
			}
			if content, err := os.ReadFile(filepath.Join(dir, entry.Name())); err == nil {
				values = append(values, string(content))
			}
		}
	}
	return values
}
