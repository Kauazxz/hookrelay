package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func envMap(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadExampleWithEnvOverrides(t *testing.T) {
	cfg, err := Load("../../config.example.json", envMap(map[string]string{
		"HOOKRELAY_SECRET_PAYMENTS": "whsec_from_env",
		"HOOKRELAY_TARGET_PAYMENTS": "https://api.example.com/hooks",
		"HOOKRELAY_ADMIN_TOKEN":     "tok",
		"HOOKRELAY_STORE":           "memory",
		"HOOKRELAY_WORKERS":         "9",
	}))
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Sources["payments"]
	if p.Secret != "whsec_from_env" || p.TargetURL != "https://api.example.com/hooks" || p.Tolerance.D() != 5*time.Minute {
		t.Fatalf("source not merged: %+v", p)
	}
	if cfg.AdminToken != "tok" || cfg.Store.Type != "memory" || cfg.Dispatcher.Workers != 9 || cfg.Dispatcher.BackoffMax.D() != 5*time.Minute {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}

func TestLoadRejectsBadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	write := func(s string) { os.WriteFile(path, []byte(s), 0o600) }

	// The example has no secret: it must come from env.
	if _, err := Load("../../config.example.json", envMap(nil)); err == nil || !strings.Contains(err.Error(), "HOOKRELAY_SECRET_PAYMENTS") {
		t.Fatalf("missing secret not reported: %v", err)
	}
	write(`{"sources": {"x": {"secret": "s", "signature_header": "X-Sig", "target_url": "ftp://nope"}}}`)
	if _, err := Load(path, envMap(nil)); err == nil || !strings.Contains(err.Error(), "target_url") {
		t.Fatalf("bad target not reported: %v", err)
	}
	write(`{"adr": ":1"}`)
	if _, err := Load(path, envMap(nil)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("typo not reported: %v", err)
	}
}
