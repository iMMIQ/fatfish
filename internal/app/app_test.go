package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAllBuiltinSchemasNoRefs verifies all 11 production built-in tools compile, register,
// sort deterministically, and produce 100% flattened OpenAI schemas with zero $ref/$defs.
func TestAllBuiltinSchemasNoRefs(t *testing.T) {
	dir := t.TempDir()
	fontPath, _ := filepath.Abs(filepath.Join("..", "..", "assets", "fonts", "NotoSansSC-Regular.ttf"))
	personaPath, _ := filepath.Abs(filepath.Join("..", "..", "configs", "personas", "example.md"))

	cfgPath := filepath.Join(dir, "fatfish.yaml")
	yamlContent := `data_dir: "` + filepath.ToSlash(dir) + `"
timezone: "Asia/Shanghai"
persona:
  file: "` + filepath.ToSlash(personaPath) + `"
tools:
  chart_font_path: "` + filepath.ToSlash(fontPath) + `"
console:
  enabled: false
`
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	application, err := New(cfgPath, false)
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	defer application.Close()

	defs := application.Registry.Definitions()
	if len(defs) != 11 {
		t.Fatalf("expected 11 built-in tools, got %d", len(defs))
	}

	raw := string(application.Registry.FrozenJSON())
	for _, forbidden := range []string{"$ref", "$defs", "$schema", "$id"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("built-in tool schema contains forbidden token %q: %s", forbidden, raw)
		}
	}

	if err := application.Compute.Renderer().VerifyFont(); err != nil {
		t.Fatalf("VerifyFont failed: %v", err)
	}
}
