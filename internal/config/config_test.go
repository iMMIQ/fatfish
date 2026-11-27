package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvExpansionAndValidation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TEST_FATFISH_KEY", "secret-key-val")
	personaPath := filepath.Join(dir, "persona.md")
	_ = os.WriteFile(personaPath, []byte("initial persona"), 0o600)

	cfgPath := filepath.Join(dir, "fatfish.yaml")
	raw := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "Asia/Shanghai"
models:
  primary:
    base_url: "https://example.com/v1"
    api_key: "${TEST_FATFISH_KEY}"
    model: "m1"
persona:
  file: "` + filepath.ToSlash(personaPath) + `"
`
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	mgr, err := NewManager(cfgPath, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if got := mgr.Get().Models.Primary.APIKey; got != "secret-key-val" {
		t.Fatalf("expected env expanded api_key, got %q", got)
	}

	// Invalid TrimLowWater >= TrimHighWater must be rejected (never silently masked by defaults).
	bad := []byte(raw + "frame:\n  trim_high_water: 0.70\n  trim_low_water: 0.85\n")
	if _, err := mgr.SaveAndReload(bad); err == nil {
		t.Fatalf("expected invalid trim watermarks to fail validation")
	}

	// Custom validator rejection must leave live config untouched.
	mgr.validator = func(c *Config) error {
		if c.Models.Primary.Model == "bad" {
			return errors.New("rejected model")
		}
		return nil
	}
	if _, err := mgr.SaveAndReload([]byte(`models:
  primary:
    model: "bad"
`)); err == nil {
		t.Fatalf("expected validator error")
	}
	if mgr.Get().Models.Primary.Model != "m1" {
		t.Fatalf("expected live config unchanged after rejected reload, got %q", mgr.Get().Models.Primary.Model)
	}

	// SavePersona updates PersonaHash atomically.
	oldHash := mgr.Get().PersonaHash
	updated, err := mgr.SavePersona("updated persona text")
	if err != nil {
		t.Fatalf("SavePersona: %v", err)
	}
	if updated.PersonaHash == oldHash || updated.PersonaText != "updated persona text" {
		t.Fatalf("expected PersonaHash and PersonaText to update")
	}
}
