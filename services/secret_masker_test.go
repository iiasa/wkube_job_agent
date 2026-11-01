package services

import (
	"bytes"
	"encoding/base64"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testSecret = "ghp_secretValue123"

func TestSecretMaskerMasksValueAndEncodings(t *testing.T) {
	var out bytes.Buffer
	m := NewSecretMasker(&out, []string{testSecret, "abc"})

	m.Write([]byte("token=" + testSecret + "\n"))
	m.Write([]byte("b64=" + base64.StdEncoding.EncodeToString([]byte(testSecret)) + "\n"))
	m.Write([]byte("url=" + url.QueryEscape(testSecret+"/x") + "\n"))
	m.Write([]byte("abc is too short to mask\n"))
	m.Flush()

	got := out.String()
	if strings.Contains(got, testSecret) || strings.Contains(got, base64.StdEncoding.EncodeToString([]byte(testSecret))) {
		t.Fatalf("secret leaked: %q", got)
	}
	if !strings.Contains(got, "token=***\n") || !strings.Contains(got, "b64=***\n") {
		t.Fatalf("missing mask: %q", got)
	}
	if !strings.Contains(got, "abc is too short to mask") {
		t.Fatalf("short values must not be masked: %q", got)
	}
}

func TestSecretMaskerCatchesValueSplitAcrossWrites(t *testing.T) {
	var out bytes.Buffer
	m := NewSecretMasker(&out, []string{testSecret})

	m.Write([]byte("start " + testSecret[:7]))
	if strings.Contains(out.String(), testSecret[:7]) {
		t.Fatalf("partial secret should be held back, got %q", out.String())
	}
	m.Write([]byte(testSecret[7:] + " end\n"))
	m.Flush()

	if got := out.String(); got != "start *** end\n" {
		t.Fatalf("got %q", got)
	}
}

func TestSecretMaskerFlushReleasesHeldTail(t *testing.T) {
	var out bytes.Buffer
	m := NewSecretMasker(&out, []string{testSecret})

	m.Write([]byte("progress ghp_"))
	m.Flush()

	if got := out.String(); got != "progress ghp_" {
		t.Fatalf("got %q", got)
	}
}

func TestSecretMaskerPassesThroughWithoutSecrets(t *testing.T) {
	var out bytes.Buffer
	m := NewSecretMasker(&out, nil)
	m.Write([]byte("plain\n"))
	if out.String() != "plain\n" {
		t.Fatalf("got %q", out.String())
	}
}

func TestLoadSecretsToMask(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "pat.txt"), []byte("file-secret"), 0o400)
	os.MkdirAll(filepath.Join(dir, "..data"), 0o700)

	t.Setenv("WAGT_MASK_ENV", "GIT_PAT, ACC_JOB_API_REFRESH_TOKEN")
	t.Setenv("GIT_PAT", "env-secret")
	t.Setenv("ACC_JOB_API_REFRESH_TOKEN", "token-secret")
	t.Setenv("WAGT_MASK_FILES_DIR", dir)

	got := strings.Join(LoadSecretsToMask(), ",")
	if got != "env-secret,token-secret,file-secret" {
		t.Fatalf("got %q", got)
	}
}
