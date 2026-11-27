package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
)

// CanonicalBlob describes a deterministically encoded image stored in BlobStore.
type CanonicalBlob struct {
	SHA256   string
	MimeType string // "image/jpeg" | "image/png"
	Ext      string // ".jpg" | ".png"
	Width    int
	Height   int
	ByteSize int64
	Path     string
}

// BlobStore manages SHA-256 content-addressed canonical images on disk.
type BlobStore struct {
	dir         string
	maxDim      int
	maxImageMiB int
}

// NewBlobStore creates a BlobStore rooted at dir.
func NewBlobStore(dir string, maxDim, maxImageMiB int) (*BlobStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir blobs %s: %w", dir, err)
	}
	if maxDim <= 0 {
		maxDim = 1280
	}
	if maxImageMiB <= 0 {
		maxImageMiB = 10
	}
	return &BlobStore{
		dir:         dir,
		maxDim:      maxDim,
		maxImageMiB: maxImageMiB,
	}, nil
}

// PutImage decodes JPEG/PNG/GIF/WebP raw bytes, resizes if larger than maxDim,
// and deterministically encodes ONCE as JPEG (q=85) or PNG (when alpha is non-opaque).
// Guarantees bit-identical base64 across KV cache turns and zero WebP API rejections.
func (b *BlobStore) PutImage(raw []byte) (CanonicalBlob, error) {
	maxBytes := int64(b.maxImageMiB) << 20
	if int64(len(raw)) > maxBytes {
		return CanonicalBlob{}, fmt.Errorf("store: image exceeds %d MiB limit", b.maxImageMiB)
	}
	if len(raw) == 0 {
		return CanonicalBlob{}, errors.New("store: empty image")
	}

	var img image.Image
	var err error
	isAnimatedGIF := false
	// For animated GIF, take first representative frame cleanly for LLM while preserving raw GIF for send_media.
	if bytes.HasPrefix(raw, []byte("GIF8")) {
		g, gerr := gif.DecodeAll(bytes.NewReader(raw))
		if gerr == nil && len(g.Image) > 0 {
			img = g.Image[0]
			if len(g.Image) > 1 {
				isAnimatedGIF = true
			}
		}
	}
	if img == nil {
		img, _, err = image.Decode(bytes.NewReader(raw))
		if err != nil {
			return CanonicalBlob{}, fmt.Errorf("store: decode image: %w", err)
		}
	}

	bounds := img.Bounds()
	w, h := bounds.Dx(), bounds.Dy()
	if w <= 0 || h <= 0 {
		return CanonicalBlob{}, errors.New("store: zero-dimension image")
	}

	// Deterministic resize if longest edge exceeds maxDim.
	if w > b.maxDim || h > b.maxDim {
		var nw, nh int
		if w >= h {
			nw = b.maxDim
			nh = int(float64(h) * float64(b.maxDim) / float64(w))
		} else {
			nh = b.maxDim
			nw = int(float64(w) * float64(b.maxDim) / float64(h))
		}
		if nw < 1 {
			nw = 1
		}
		if nh < 1 {
			nh = 1
		}
		dst := image.NewNRGBA(image.Rect(0, 0, nw, nh))
		draw.CatmullRom.Scale(dst, dst.Bounds(), img, bounds, draw.Over, nil)
		img = dst
		w, h = nw, nh
	}

	hasAlpha := imageHasAlpha(img)
	var out bytes.Buffer
	var mimeType, ext string
	if hasAlpha {
		enc := png.Encoder{CompressionLevel: png.DefaultCompression}
		if err := enc.Encode(&out, img); err != nil {
			return CanonicalBlob{}, fmt.Errorf("store: encode png: %w", err)
		}
		mimeType = "image/png"
		ext = ".png"
	} else {
		if err := jpeg.Encode(&out, img, &jpeg.Options{Quality: 85}); err != nil {
			return CanonicalBlob{}, fmt.Errorf("store: encode jpeg: %w", err)
		}
		mimeType = "image/jpeg"
		ext = ".jpg"
	}

	encoded := out.Bytes()
	sum := sha256.Sum256(encoded)
	hash := hex.EncodeToString(sum[:])

	subDir := filepath.Join(b.dir, hash[:2])
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		return CanonicalBlob{}, err
	}
	dstPath := filepath.Join(subDir, hash+ext)
	if _, err := os.Stat(dstPath); os.IsNotExist(err) {
		if err := writeFileAtomic(subDir, dstPath, encoded); err != nil {
			return CanonicalBlob{}, err
		}
	}
	totalSize := int64(len(encoded))
	if isAnimatedGIF {
		rawGifPath := filepath.Join(subDir, hash+".raw.gif")
		if _, err := os.Stat(rawGifPath); os.IsNotExist(err) {
			if err := writeFileAtomic(subDir, rawGifPath, raw); err == nil {
				totalSize += int64(len(raw))
			}
		} else if st, err := os.Stat(rawGifPath); err == nil {
			totalSize += st.Size()
		}
	}

	return CanonicalBlob{
		SHA256:   hash,
		MimeType: mimeType,
		Ext:      ext,
		Width:    w,
		Height:   h,
		ByteSize: totalSize,
		Path:     dstPath,
	}, nil
}

func writeFileAtomic(dir, dstPath string, data []byte) error {
	f, err := os.CreateTemp(dir, ".blob-*.tmp")
	if err != nil {
		return err
	}
	tmpName := f.Name()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := f.Chmod(0o644); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, dstPath); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// PathFor resolves the canonical file path for a sha256 + mimeType pair.
func (b *BlobStore) PathFor(sha256Hex, mimeType string) string {
	if len(sha256Hex) < 2 {
		return ""
	}
	ext := ".jpg"
	if strings.Contains(mimeType, "png") {
		ext = ".png"
	}
	return filepath.Join(b.dir, sha256Hex[:2], sha256Hex+ext)
}

// ReadBytes returns the canonical image bytes for sha256 + mimeType.
func (b *BlobStore) ReadBytes(sha256Hex, mimeType string) ([]byte, error) {
	p := b.PathFor(sha256Hex, mimeType)
	if p == "" {
		return nil, errors.New("store: invalid sha256")
	}
	return os.ReadFile(p)
}

// ReadMediaBytes returns the preserved raw animated GIF if present (for send_media),
// otherwise returns the canonical JPEG/PNG bytes and mimeType.
func (b *BlobStore) ReadMediaBytes(sha256Hex, mimeType string) ([]byte, string, error) {
	if len(sha256Hex) < 2 {
		return nil, "", errors.New("store: invalid sha256")
	}
	rawGifPath := filepath.Join(b.dir, sha256Hex[:2], sha256Hex+".raw.gif")
	if raw, err := os.ReadFile(rawGifPath); err == nil && len(raw) > 0 {
		return raw, "image/gif", nil
	}
	raw, err := b.ReadBytes(sha256Hex, mimeType)
	return raw, mimeType, err
}

// DataURI returns the deterministic base64 data URI ("data:image/jpeg;base64,...") for LLM payloads.
func (b *BlobStore) DataURI(sha256Hex, mimeType string) (string, error) {
	raw, err := b.ReadBytes(sha256Hex, mimeType)
	if err != nil {
		return "", err
	}
	return "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(raw), nil
}

// Delete removes a blob file (and any preserved animated GIF companion) if it exists.
func (b *BlobStore) Delete(sha256Hex, mimeType string) error {
	p := b.PathFor(sha256Hex, mimeType)
	if p == "" {
		return nil
	}
	rawGifPath := filepath.Join(b.dir, sha256Hex[:2], sha256Hex+".raw.gif")
	_ = os.Remove(rawGifPath)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func imageHasAlpha(img image.Image) bool {
	type opaquer interface {
		Opaque() bool
	}
	if op, ok := img.(opaquer); ok {
		return !op.Opaque()
	}
	b := img.Bounds()
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			_, _, _, a := img.At(x, y).RGBA()
			if a != 0xffff {
				return true
			}
		}
	}
	return false
}
