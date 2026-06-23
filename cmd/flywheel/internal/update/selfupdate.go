package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	iver "github.com/mrz1836/go-flywheel/cmd/flywheel/internal/version"
)

// Self-update guards.
const (
	// maxAssetSize caps a downloaded release archive (defense against a huge body).
	maxAssetSize = 256 << 20 // 256 MiB
	// maxChecksumSize caps the checksums.txt read.
	maxChecksumSize = 1 << 20 // 1 MiB
	// maxBinarySize caps a single extracted file (zip-bomb defense).
	maxBinarySize = 256 << 20 // 256 MiB
	// downloadTimeout bounds the whole download phase.
	downloadTimeout = 5 * time.Minute
	// defaultBinaryName is the executable name inside the release archive.
	defaultBinaryName = "flywheel"
)

// osExecutable resolves the running binary's path. It is a variable so tests can
// point SelfUpdate's default target at a temp file instead of the test binary,
// without changing production behavior (it defaults to os.Executable).
//
//nolint:gochecknoglobals // injectable seam for tests; defaults to os.Executable
var osExecutable = os.Executable

// Self-update errors.
var (
	errNoAsset          = errors.New("no release asset for this os/arch")
	errNoChecksums      = errors.New("no checksums.txt in the release")
	errChecksumMismatch = errors.New("asset checksum verification failed")
	errChecksumNotFound = errors.New("asset not listed in checksums.txt")
	errBinaryNotFound   = errors.New("flywheel binary not found in the release archive")
	errPathTraversal    = errors.New("archive entry escapes the destination directory")
	errAssetTooLarge    = errors.New("release asset exceeds the size limit")
)

// Options configures SelfUpdate.
type Options struct {
	// Force installs the latest release even when it is not newer than current.
	Force bool
	// TargetPath is the binary to replace; empty resolves to os.Executable().
	TargetPath string
	// BinaryName is the executable name inside the archive; empty means "flywheel".
	BinaryName string
	// Client is the HTTP client for downloads; nil uses a bounded default.
	Client *http.Client
	// Out receives human-readable progress; nil discards it.
	Out io.Writer
}

// SelfUpdate fetches the latest release and, when it is newer (or Force is set),
// downloads the OS/arch asset, verifies its SHA256 against the release
// checksums.txt, extracts the flywheel binary, and atomically replaces the
// target binary. It returns the latest tag and whether a replace happened.
func SelfUpdate(ctx context.Context, currentVersion string, fetcher ReleaseFetcher, opts Options) (string, bool, error) {
	out := opts.Out
	if out == nil {
		out = io.Discard
	}
	rel, err := fetcher.Latest(ctx)
	if err != nil {
		return "", false, err
	}
	latest := rel.TagName
	if !opts.Force && !iver.IsNewer(currentVersion, latest) {
		return latest, false, nil
	}

	asset, checksumURL, err := selectAsset(rel)
	if err != nil {
		return latest, false, err
	}
	_, _ = fmt.Fprintf(out, "downloading %s\n", asset.Name)

	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: downloadTimeout}
	}
	dlCtx, cancel := context.WithTimeout(ctx, downloadTimeout)
	defer cancel()

	data, err := download(dlCtx, client, asset.URL, maxAssetSize)
	if err != nil {
		return latest, false, err
	}
	if err := verifyChecksum(dlCtx, client, data, asset.Name, checksumURL); err != nil {
		return latest, false, err
	}
	_, _ = fmt.Fprintln(out, "checksum verified")

	target := opts.TargetPath
	if target == "" {
		exe, exeErr := osExecutable()
		if exeErr != nil {
			return latest, false, fmt.Errorf("resolve running binary: %w", exeErr)
		}
		if resolved, linkErr := filepath.EvalSymlinks(exe); linkErr == nil {
			exe = resolved
		}
		target = exe
	}

	binName := opts.BinaryName
	if binName == "" {
		binName = defaultBinaryName
	}

	tmpDir, err := os.MkdirTemp("", "flywheel-update-*")
	if err != nil {
		return latest, false, fmt.Errorf("create update dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	binPath, err := extractBinary(data, binName, tmpDir)
	if err != nil {
		return latest, false, err
	}
	if err := replaceBinary(binPath, target); err != nil {
		return latest, false, err
	}

	_ = ClearCache()
	_, _ = fmt.Fprintf(out, "updated %s -> %s\n", currentVersion, latest)
	return latest, true, nil
}

// selectAsset returns the release asset for this OS/arch (matched by the
// goreleaser "_<os>_<arch>.tar.gz" suffix) and the checksums.txt download URL.
func selectAsset(rel *Release) (Asset, string, error) {
	suffix := fmt.Sprintf("_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	var asset Asset
	var checksumURL string
	for _, a := range rel.Assets {
		switch {
		case strings.HasSuffix(a.Name, suffix):
			asset = a
		case strings.HasSuffix(a.Name, "checksums.txt"):
			checksumURL = a.URL
		}
	}
	if asset.URL == "" {
		return Asset{}, "", fmt.Errorf("%w: %s", errNoAsset, suffix)
	}
	if checksumURL == "" {
		return Asset{}, "", errNoChecksums
	}
	return asset, checksumURL, nil
}

// download fetches url into memory, capping the read at maxBytes.
func download(ctx context.Context, client *http.Client, url string, maxBytes int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create download request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download asset: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download asset: status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read asset: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, errAssetTooLarge
	}
	return data, nil
}

// verifyChecksum fetches checksumURL and confirms data's SHA256 matches the
// entry for assetName (goreleaser "<sha256>  <filename>" format).
func verifyChecksum(ctx context.Context, client *http.Client, data []byte, assetName, checksumURL string) error {
	sums, err := download(ctx, client, checksumURL, maxChecksumSize)
	if err != nil {
		return fmt.Errorf("fetch checksums: %w", err)
	}
	want := ""
	for _, line := range strings.Split(string(sums), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == assetName && len(fields[0]) == 64 {
			want = strings.ToLower(fields[0])
			break
		}
	}
	if want == "" {
		return fmt.Errorf("%w: %s", errChecksumNotFound, assetName)
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	if got != want {
		return fmt.Errorf("%w: want %s, got %s", errChecksumMismatch, want, got)
	}
	return nil
}

// extractBinary extracts the entry named binaryName from a tar.gz into destDir,
// guarding against path traversal and oversized entries, and returns its path.
func extractBinary(data []byte, binaryName, destDir string) (string, error) {
	return extractBinaryWithCap(data, binaryName, destDir, maxBinarySize)
}

// extractBinaryWithCap is the core of extractBinary with the per-file size cap as
// a parameter. extractBinary supplies maxBinarySize; tests supply a small cap to
// exercise the oversized-entry guard without allocating a huge archive. Behavior
// is identical to the previous inline implementation.
func extractBinaryWithCap(data []byte, binaryName, destDir string, maxBytes int64) (string, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("open gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("read archive: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg || filepath.Base(hdr.Name) != binaryName {
			continue
		}
		destPath, err := safeJoin(destDir, hdr.Name)
		if err != nil {
			return "", err
		}
		if mkErr := os.MkdirAll(filepath.Dir(destPath), 0o700); mkErr != nil {
			return "", fmt.Errorf("create extract dir: %w", mkErr)
		}
		f, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o700) //nolint:gosec // destPath validated by safeJoin
		if err != nil {
			return "", fmt.Errorf("create extracted file: %w", err)
		}
		n, copyErr := io.Copy(f, io.LimitReader(tr, maxBytes+1))
		closeErr := f.Close()
		if copyErr != nil {
			return "", fmt.Errorf("extract binary: %w", copyErr)
		}
		if closeErr != nil {
			return "", fmt.Errorf("close extracted file: %w", closeErr)
		}
		if n > maxBytes {
			_ = os.Remove(destPath)
			return "", errAssetTooLarge
		}
		return destPath, nil
	}
	return "", fmt.Errorf("%w: %s", errBinaryNotFound, binaryName)
}

// safeJoin joins name onto destDir, rejecting entries that escape destDir
// (absolute paths or ".." traversal — the Zip Slip defense).
func safeJoin(destDir, name string) (string, error) {
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("%w: %s", errPathTraversal, name)
	}
	destDir = filepath.Clean(destDir)
	target := filepath.Clean(filepath.Join(destDir, name))
	rel, err := filepath.Rel(destDir, target)
	if err != nil {
		return "", fmt.Errorf("resolve archive path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %s", errPathTraversal, name)
	}
	return target, nil
}

// replaceBinary atomically replaces dst with the binary at src: it copies src to
// a temp file beside dst (so the rename stays on one filesystem), preserves dst's
// permissions when it exists, then renames over dst. The rename keeps a running
// binary safe — the old inode survives for the live process.
func replaceBinary(src, dst string) error {
	mode := os.FileMode(0o755)
	if info, err := os.Stat(dst); err == nil {
		mode = info.Mode().Perm()
	}
	in, err := os.Open(src) //nolint:gosec // src is our own extracted file
	if err != nil {
		return fmt.Errorf("open new binary: %w", err)
	}
	defer func() { _ = in.Close() }()

	tmp := dst + ".new"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode) //nolint:gosec // tmp derived from dst
	if err != nil {
		return fmt.Errorf("create temp binary: %w", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write temp binary: %w", err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("flush temp binary: %w", err)
	}
	if err := os.Chmod(tmp, mode); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("chmod temp binary: %w", err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("install binary: %w", err)
	}
	return nil
}
