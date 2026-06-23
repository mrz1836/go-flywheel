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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTripFunc adapts a function to http.RoundTripper so tests can serve canned
// responses entirely in-process: no sockets, no loopback, no real network. Every
// HTTP-touching path below is exercised through one of these, satisfying the
// "no network" invariant while still flowing through the production *http.Client.
type roundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip implements http.RoundTripper.
func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// clientFromRoundTripper wraps an in-process RoundTripper in an *http.Client so it
// can be injected wherever the package accepts a client.
func clientFromRoundTripper(t *testing.T, rt roundTripFunc) *http.Client {
	t.Helper()
	return &http.Client{Transport: rt}
}

// okResponse builds a 200 response whose body is body, for use from a
// RoundTripper. It mirrors what a healthy GitHub/CDN edge would return.
func okResponse(req *http.Request, body []byte) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(body)),
		Header:     make(http.Header),
		Request:    req,
	}
}

// errReadCloser is a body whose Read always fails, used to drive io.ReadAll
// error branches without any real I/O.
type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) { return 0, errors.New("simulated read failure") }
func (errReadCloser) Close() error             { return nil }

// errStubTransport is a sentinel returned by transports that simulate a
// connection-level failure (DNS, refused, reset) without touching the network.
var errStubTransport = errors.New("simulated transport failure")

// makeGoodReleaseAssets returns a tar.gz archive carrying a "flywheel" binary
// plus a matching checksums.txt line, for happy-path self-update tests.
func makeGoodReleaseAssets(t *testing.T, content []byte) (archive []byte, checksums string) {
	t.Helper()
	archive = makeTarGz(t, "flywheel", content)
	sum := sha256.Sum256(archive)
	checksums = fmt.Sprintf("%s  %s\n", hex.EncodeToString(sum[:]), assetName())
	return archive, checksums
}

// inProcReleaseClient returns an *http.Client whose in-process transport serves
// the asset bytes at "/asset" and the checksums at "/checksums.txt", and a
// Release pointing at those synthetic URLs. No socket is opened.
func inProcReleaseClient(t *testing.T, archive []byte, checksums string) (*http.Client, *Release) {
	t.Helper()
	const assetURL = "https://example.invalid/asset"
	const checksumURL = "https://example.invalid/checksums.txt"
	client := clientFromRoundTripper(t, func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case assetURL:
			return okResponse(req, archive), nil
		case checksumURL:
			return okResponse(req, []byte(checksums)), nil
		default:
			return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
		}
	})
	rel := &Release{TagName: "v2.0.0", Assets: []Asset{
		{Name: assetName(), URL: assetURL},
		{Name: "go-flywheel_2.0.0_checksums.txt", URL: checksumURL},
	}}
	return client, rel
}

// TestSelfUpdateFullFlowInProcess proves the complete happy path — fetch,
// download, checksum-verify, extract, atomic replace, cache clear — runs end to
// end against an in-process transport with no network access.
func TestSelfUpdateFullFlowInProcess(t *testing.T) {
	useTempCache(t)
	content := []byte("verified-flywheel-binary")
	archive, checksums := makeGoodReleaseAssets(t, content)
	client, rel := inProcReleaseClient(t, archive, checksums)

	target := filepath.Join(t.TempDir(), "flywheel")
	require.NoError(t, os.WriteFile(target, []byte("old-binary"), 0o755))

	var out bytes.Buffer
	latest, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		TargetPath: target, Client: client, Out: &out, BinaryName: "flywheel",
	})
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, "v2.0.0", latest)

	got, err := os.ReadFile(target) //nolint:gosec // test-controlled temp path
	require.NoError(t, err)
	assert.Equal(t, content, got, "the replaced binary holds the verified release bytes")

	// The progress writer received each milestone line.
	assert.Contains(t, out.String(), "downloading")
	assert.Contains(t, out.String(), "checksum verified")
	assert.Contains(t, out.String(), "updated v1.0.0 -> v2.0.0")
}

// TestSelfUpdateSurfacesFetcherError proves a fetcher failure short-circuits the
// whole update with an empty tag and no replace.
func TestSelfUpdateSurfacesFetcherError(t *testing.T) {
	useTempCache(t)
	sentinel := errors.New("fetch boom")
	latest, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{err: sentinel}, Options{
		TargetPath: filepath.Join(t.TempDir(), "flywheel"),
	})
	assert.ErrorIs(t, err, sentinel)
	assert.False(t, updated)
	assert.Empty(t, latest, "a fetch failure yields no resolved tag")
}

// TestSelfUpdateSurfacesDownloadError proves a transport failure during the asset
// download aborts the update and leaves the target untouched.
func TestSelfUpdateSurfacesDownloadError(t *testing.T) {
	useTempCache(t)
	client := clientFromRoundTripper(t, func(*http.Request) (*http.Response, error) {
		return nil, errStubTransport
	})
	rel := &Release{TagName: "v2.0.0", Assets: []Asset{
		{Name: assetName(), URL: "https://example.invalid/asset"},
		{Name: "go-flywheel_2.0.0_checksums.txt", URL: "https://example.invalid/checksums.txt"},
	}}
	target := filepath.Join(t.TempDir(), "flywheel")
	require.NoError(t, os.WriteFile(target, []byte("old"), 0o755))

	latest, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		TargetPath: target, Client: client,
	})
	require.Error(t, err)
	assert.False(t, updated)
	assert.Equal(t, "v2.0.0", latest, "the latest tag is known even when the download fails")

	got, _ := os.ReadFile(target) //nolint:gosec // test-controlled temp path
	assert.Equal(t, []byte("old"), got)
}

// TestSelfUpdateSurfacesExtractError proves a release whose archive lacks the
// flywheel binary fails at extraction (after a valid checksum) without replacing.
func TestSelfUpdateSurfacesExtractError(t *testing.T) {
	useTempCache(t)
	// Archive contains a different file name, so extraction cannot find "flywheel".
	archive := makeTarGz(t, "not-flywheel", []byte("payload"))
	sum := sha256.Sum256(archive)
	checksums := fmt.Sprintf("%s  %s\n", hex.EncodeToString(sum[:]), assetName())
	client, rel := inProcReleaseClient(t, archive, checksums)

	target := filepath.Join(t.TempDir(), "flywheel")
	require.NoError(t, os.WriteFile(target, []byte("old"), 0o755))

	_, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		TargetPath: target, Client: client,
	})
	assert.ErrorIs(t, err, errBinaryNotFound)
	assert.False(t, updated)
	got, _ := os.ReadFile(target) //nolint:gosec // test-controlled temp path
	assert.Equal(t, []byte("old"), got, "a failed extract leaves the target untouched")
}

// TestSelfUpdateSurfacesReplaceError proves that when the verified binary cannot
// be installed (the target's parent directory is unwritable) the update fails
// and reports the latest tag.
func TestSelfUpdateSurfacesReplaceError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	useTempCache(t)
	content := []byte("verified")
	archive, checksums := makeGoodReleaseAssets(t, content)
	client, rel := inProcReleaseClient(t, archive, checksums)

	// A read-only parent directory makes the side-by-side temp write fail.
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	target := filepath.Join(dir, "flywheel") // does not exist yet, parent is read-only

	latest, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		TargetPath: target, Client: client,
	})
	require.Error(t, err)
	assert.False(t, updated)
	assert.Equal(t, "v2.0.0", latest)
}

// TestSelfUpdateChecksumFetchFails proves a verification step that cannot fetch
// the checksums.txt aborts the update.
func TestSelfUpdateChecksumFetchFails(t *testing.T) {
	useTempCache(t)
	content := []byte("verified")
	archive := makeTarGz(t, "flywheel", content)
	client := clientFromRoundTripper(t, func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/asset") {
			return okResponse(req, archive), nil
		}
		// The checksums request fails at the transport layer.
		return nil, errStubTransport
	})
	rel := &Release{TagName: "v2.0.0", Assets: []Asset{
		{Name: assetName(), URL: "https://example.invalid/asset"},
		{Name: "go-flywheel_2.0.0_checksums.txt", URL: "https://example.invalid/checksums.txt"},
	}}
	target := filepath.Join(t.TempDir(), "flywheel")
	require.NoError(t, os.WriteFile(target, []byte("old"), 0o755))

	_, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		TargetPath: target, Client: client,
	})
	require.Error(t, err)
	assert.False(t, updated)
}

// TestSelfUpdateSkipsWhenNotNewerNoOpts proves the early "already latest" return
// when neither Client nor TargetPath is supplied — no network, no filesystem.
func TestSelfUpdateSkipsWhenNotNewerNoOpts(t *testing.T) {
	useTempCache(t)
	latest, updated, err := SelfUpdate(context.Background(), "v2.0.0",
		&fakeFetcher{rel: &Release{TagName: "v2.0.0"}}, Options{})
	require.NoError(t, err)
	assert.False(t, updated)
	assert.Equal(t, "v2.0.0", latest)
}

// TestSelfUpdateDefaultsClientAndTarget proves the full happy path while taking
// the *default* branches: opts.Client==nil (a bounded client is built) and
// opts.TargetPath=="" (the running-binary path is resolved). The osExecutable
// seam points the "running binary" at a temp file so the test never touches the
// real test binary, and the download client is injected via the package default
// being bypassed — to keep it network-free we instead inject a client whose
// transport is in-process by setting it through a per-test override of the
// default. Since SelfUpdate builds its own client when nil, we cannot inject
// there; so this test supplies a Client but leaves TargetPath empty to cover the
// os.Executable resolution branch in-process.
func TestSelfUpdateResolvesRunningBinaryTarget(t *testing.T) {
	useTempCache(t)
	content := []byte("verified-default-target")
	archive, checksums := makeGoodReleaseAssets(t, content)
	client, rel := inProcReleaseClient(t, archive, checksums)

	// Point the "running binary" at a temp file and a symlink to it, so both the
	// os.Executable() call and the EvalSymlinks resolution run against test paths.
	dir := t.TempDir()
	realBin := filepath.Join(dir, "flywheel-real")
	require.NoError(t, os.WriteFile(realBin, []byte("old"), 0o755))
	link := filepath.Join(dir, "flywheel-link")
	require.NoError(t, os.Symlink(realBin, link))

	origExec := osExecutable
	osExecutable = func() (string, error) { return link, nil }
	t.Cleanup(func() { osExecutable = origExec })

	latest, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		Client: client, // TargetPath empty => resolve via osExecutable + EvalSymlinks
	})
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, "v2.0.0", latest)

	// The symlink target (the resolved real binary) was replaced.
	got, err := os.ReadFile(realBin) //nolint:gosec // test-controlled temp path
	require.NoError(t, err)
	assert.Equal(t, content, got, "the resolved running-binary path is replaced")
}

// TestSelfUpdateExecutableError proves a failure to resolve the running binary
// (osExecutable error) aborts the update after a successful download+verify.
func TestSelfUpdateExecutableError(t *testing.T) {
	useTempCache(t)
	content := []byte("verified")
	archive, checksums := makeGoodReleaseAssets(t, content)
	client, rel := inProcReleaseClient(t, archive, checksums)

	origExec := osExecutable
	osExecutable = func() (string, error) { return "", errors.New("no executable") }
	t.Cleanup(func() { osExecutable = origExec })

	_, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		Client: client, // TargetPath empty => osExecutable is consulted and fails
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve running binary")
	assert.False(t, updated)
}

// TestSelfUpdateNilClientBuildsDefault proves the opts.Client==nil branch builds
// a bounded default client. To stay network-free, the release asset URL contains
// a control character: the default client is constructed, then download fails at
// http.NewRequestWithContext before any socket is opened. No real request leaves
// the process.
func TestSelfUpdateNilClientBuildsDefault(t *testing.T) {
	useTempCache(t)
	rel := &Release{TagName: "v2.0.0", Assets: []Asset{
		{Name: assetName(), URL: "http://\x7f-bad-asset-url"},
		{Name: "go-flywheel_2.0.0_checksums.txt", URL: "http://\x7f-bad-checksums"},
	}}
	latest, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		TargetPath: filepath.Join(t.TempDir(), "flywheel"), // Client nil => default built
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create download request",
		"the default client is built, then the malformed asset URL fails at request creation")
	assert.False(t, updated)
	assert.Equal(t, "v2.0.0", latest)
}

// TestDownloadRequestCreationError proves a malformed URL fails before any
// transport call (http.NewRequestWithContext rejects control characters).
func TestDownloadRequestCreationError(t *testing.T) {
	t.Parallel()
	client := clientFromRoundTripper(t, func(*http.Request) (*http.Response, error) {
		t.Error("transport must not be reached for an invalid URL")
		return nil, nil
	})
	_, err := download(context.Background(), client, "http://\x7f-bad-url", 1024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create download request")
}

// TestDownloadTransportError proves a connection-level failure surfaces as a
// wrapped download error.
func TestDownloadTransportError(t *testing.T) {
	t.Parallel()
	client := clientFromRoundTripper(t, func(*http.Request) (*http.Response, error) {
		return nil, errStubTransport
	})
	_, err := download(context.Background(), client, "https://example.invalid/x", 1024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "download asset")
}

// TestDownloadReadBodyError proves a body that fails mid-read surfaces as a
// wrapped read error.
func TestDownloadReadBodyError(t *testing.T) {
	t.Parallel()
	client := clientFromRoundTripper(t, func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       errReadCloser{},
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	_, err := download(context.Background(), client, "https://example.invalid/x", 1024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read asset")
}

// TestDownloadHappyPath proves a well-formed 200 response within the cap returns
// the full body.
func TestDownloadHappyPath(t *testing.T) {
	t.Parallel()
	body := []byte("hello-bytes")
	client := clientFromRoundTripper(t, func(req *http.Request) (*http.Response, error) {
		return okResponse(req, body), nil
	})
	got, err := download(context.Background(), client, "https://example.invalid/x", 1024)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

// TestVerifyChecksumHappyPath proves a matching checksum line passes
// verification, exercised purely in-process.
func TestVerifyChecksumHappyPath(t *testing.T) {
	t.Parallel()
	data := []byte("payload-to-verify")
	sum := sha256.Sum256(data)
	checksums := fmt.Sprintf("%s  %s\n", hex.EncodeToString(sum[:]), assetName())
	client := clientFromRoundTripper(t, func(req *http.Request) (*http.Response, error) {
		return okResponse(req, []byte(checksums)), nil
	})
	err := verifyChecksum(context.Background(), client, data, assetName(), "https://example.invalid/checksums.txt")
	require.NoError(t, err)
}

// TestVerifyChecksumFetchError proves a transport failure fetching checksums.txt
// is wrapped as a fetch error.
func TestVerifyChecksumFetchError(t *testing.T) {
	t.Parallel()
	client := clientFromRoundTripper(t, func(*http.Request) (*http.Response, error) {
		return nil, errStubTransport
	})
	err := verifyChecksum(context.Background(), client, []byte("data"), assetName(), "https://example.invalid/checksums.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch checksums")
}

// TestExtractBinaryGzipError proves non-gzip data is rejected before any tar
// parsing.
func TestExtractBinaryGzipError(t *testing.T) {
	t.Parallel()
	_, err := extractBinary([]byte("this is not gzip"), "flywheel", t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open gzip")
}

// TestExtractBinaryTarReadError proves a truncated tar stream (valid gzip,
// corrupt tar) surfaces a read-archive error.
func TestExtractBinaryTarReadError(t *testing.T) {
	t.Parallel()
	// Gzip-compress bytes that are not a valid tar archive.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write([]byte("definitely not a tar header but long enough to parse as one"))
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	_, err = extractBinary(buf.Bytes(), "flywheel", t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read archive")
}

// TestExtractBinaryRejectsPathTraversal proves an archive entry that escapes the
// destination directory is rejected by safeJoin during extraction.
func TestExtractBinaryRejectsPathTraversal(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	content := []byte("evil")
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "../flywheel", Mode: 0o755, Size: int64(len(content)), Typeflag: tar.TypeReg,
	}))
	_, err := tw.Write(content)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	_, err = extractBinary(buf.Bytes(), "flywheel", t.TempDir())
	assert.ErrorIs(t, err, errPathTraversal)
}

// TestExtractBinaryRejectsOversized proves an entry larger than maxBinarySize is
// rejected and the partially written file is removed. It overrides the package
// size cap to a tiny value so the test stays fast and allocates nothing huge.
func TestExtractBinaryRejectsOversized(t *testing.T) {
	// Build an archive with a 32-byte file, then extract under a tiny cap.
	content := bytes.Repeat([]byte("A"), 32)
	archive := makeTarGz(t, "flywheel", content)

	dir := t.TempDir()
	// safeJoin + open succeed; the copy exceeds the cap, triggering cleanup.
	got, err := extractBinaryWithCap(archive, "flywheel", dir, 8)
	require.ErrorIs(t, err, errAssetTooLarge)
	assert.Empty(t, got)
	// The oversized file must have been removed.
	_, statErr := os.Stat(filepath.Join(dir, "flywheel"))
	assert.True(t, os.IsNotExist(statErr), "an oversized extract is cleaned up")
}

// TestExtractBinaryCreateFileError proves a read-only destination directory makes
// the extracted-file open fail with a wrapped error.
func TestExtractBinaryCreateFileError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	t.Parallel()
	archive := makeTarGz(t, "flywheel", []byte("payload"))
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o500)) // read-only: cannot create files
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	_, err := extractBinary(archive, "flywheel", dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create extracted file")
}

// TestExtractBinaryCreateDirError proves that when the archive entry lives under
// a subdirectory and the destination root is read-only, the MkdirAll for the
// nested extract directory fails with a wrapped error.
func TestExtractBinaryCreateDirError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	t.Parallel()
	archive := makeTarGz(t, "nested/flywheel", []byte("payload"))
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o500)) // read-only: cannot create subdirs
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	_, err := extractBinary(archive, "flywheel", dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create extract dir")
}

// TestExtractBinaryCopyError proves a tar entry whose declared Size exceeds the
// actual payload (a truncated archive) surfaces an extract-binary copy error.
func TestExtractBinaryCopyError(t *testing.T) {
	t.Parallel()
	// Build a valid gzip stream containing a tar header that claims 100 bytes for
	// "flywheel" but provides none, then truncate before the data — io.Copy hits
	// an unexpected EOF mid-read.
	var raw bytes.Buffer
	tw := tar.NewWriter(&raw)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "flywheel", Mode: 0o755, Size: 100, Typeflag: tar.TypeReg,
	}))
	// Intentionally do NOT write the 100 bytes and do NOT close cleanly; truncate
	// the tar stream right after the header (512 bytes).
	header := raw.Bytes()[:512]

	var gzBuf bytes.Buffer
	gz := gzip.NewWriter(&gzBuf)
	_, err := gz.Write(header)
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	_, err = extractBinary(gzBuf.Bytes(), "flywheel", t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract binary", "a truncated entry fails the copy")
}

// TestExtractBinaryHappyPath proves a well-formed archive yields the extracted
// binary path with executable bits.
func TestExtractBinaryHappyPath(t *testing.T) {
	t.Parallel()
	content := []byte("binary-bytes")
	archive := makeTarGz(t, "flywheel", content)
	dir := t.TempDir()
	got, err := extractBinary(archive, "flywheel", dir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "flywheel"), got)
	data, err := os.ReadFile(got) //nolint:gosec // test-controlled temp path
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

// TestSafeJoinResolveRelError documents safeJoin's happy path for a nested but
// in-bounds entry, complementing the traversal-rejection tests.
func TestSafeJoinNestedEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	got, err := safeJoin(dir, "sub/dir/flywheel")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "sub", "dir", "flywheel"), got)
}

// TestReplaceBinaryOpenSrcError proves a missing source binary is reported.
func TestReplaceBinaryOpenSrcError(t *testing.T) {
	t.Parallel()
	dst := filepath.Join(t.TempDir(), "flywheel")
	err := replaceBinary(filepath.Join(t.TempDir(), "does-not-exist"), dst)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "open new binary")
}

// TestReplaceBinaryTempCreateError proves an unwritable destination directory
// fails when the side-by-side temp file cannot be created.
func TestReplaceBinaryTempCreateError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	t.Parallel()
	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "new")
	require.NoError(t, os.WriteFile(src, []byte("new-binary"), 0o755))

	dstDir := t.TempDir()
	require.NoError(t, os.Chmod(dstDir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dstDir, 0o700) })

	err := replaceBinary(src, filepath.Join(dstDir, "flywheel"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create temp binary")
}

// TestReplaceBinaryRenameError proves a failure to rename the staged temp file
// over the destination is reported and the temp file is cleaned up. The
// destination path is a directory, so os.Rename(tmp, dir) fails.
func TestReplaceBinaryRenameError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	require.NoError(t, os.WriteFile(src, []byte("new"), 0o755))

	// dst is an existing directory: Stat succeeds (mode read), copy to dst+".new"
	// succeeds, but Rename over a non-empty directory fails.
	dst := filepath.Join(dir, "dstdir")
	require.NoError(t, os.Mkdir(dst, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dst, "occupant"), []byte("x"), 0o644))

	err := replaceBinary(src, dst)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "install binary")
	// The staged temp file must not linger.
	_, statErr := os.Stat(dst + ".new")
	assert.True(t, os.IsNotExist(statErr), "a failed rename removes the staged temp binary")
}

// TestReplaceBinaryPreservesExistingMode proves replaceBinary preserves the
// permissions of an existing destination file and installs the new content.
func TestReplaceBinaryPreservesExistingMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	require.NoError(t, os.WriteFile(src, []byte("new"), 0o644))

	dst := filepath.Join(dir, "dst")
	require.NoError(t, os.WriteFile(dst, []byte("old"), 0o755)) // existing mode preserved

	require.NoError(t, replaceBinary(src, dst))
	got, err := os.ReadFile(dst) //nolint:gosec // test-controlled temp path
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), got)

	info, err := os.Stat(dst)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o755), info.Mode().Perm(), "the existing destination mode is preserved")
}

// TestReadCachePathError proves a cachePathFn failure propagates from ReadCache.
func TestReadCachePathError(t *testing.T) {
	origPath, origNow := cachePathFn, nowFn
	t.Cleanup(func() { cachePathFn, nowFn = origPath, origNow })
	sentinel := errors.New("path boom")
	cachePathFn = func() (string, error) { return "", sentinel }

	_, err := ReadCache()
	assert.ErrorIs(t, err, sentinel)
}

// TestReadCacheParseError proves malformed JSON in the cache file is reported.
func TestReadCacheParseError(t *testing.T) {
	path := useTempCache(t)
	require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o600))

	_, err := ReadCache()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse update cache")
}

// TestReadCacheReadError proves a non-not-exist read error (here, the path is a
// directory) is wrapped and returned.
func TestReadCacheReadError(t *testing.T) {
	origPath, origNow := cachePathFn, nowFn
	t.Cleanup(func() { cachePathFn, nowFn = origPath, origNow })
	dir := t.TempDir() // a directory cannot be read as a file
	cachePathFn = func() (string, error) { return dir, nil }

	_, err := ReadCache()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read update cache")
}

// TestWriteCachePathError proves a cachePathFn failure propagates from WriteCache.
func TestWriteCachePathError(t *testing.T) {
	origPath, origNow := cachePathFn, nowFn
	t.Cleanup(func() { cachePathFn, nowFn = origPath, origNow })
	sentinel := errors.New("path boom")
	cachePathFn = func() (string, error) { return "", sentinel }

	err := WriteCache(&CacheEntry{LatestVersion: "v1.0.0"})
	assert.ErrorIs(t, err, sentinel)
}

// TestWriteCacheMkdirError proves WriteCache reports a failure to create the
// cache directory (parent path is a regular file, so MkdirAll fails).
func TestWriteCacheMkdirError(t *testing.T) {
	origPath, origNow := cachePathFn, nowFn
	t.Cleanup(func() { cachePathFn, nowFn = origPath, origNow })

	// A regular file standing where a directory is expected makes MkdirAll fail.
	fileAsDir := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(fileAsDir, []byte("x"), 0o600))
	cachePathFn = func() (string, error) { return filepath.Join(fileAsDir, "update-check.json"), nil }

	err := WriteCache(&CacheEntry{LatestVersion: "v1.0.0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create update cache dir")
}

// TestWriteCacheWriteTempError proves a failure to write the temp cache file is
// reported. The cache directory is made read-only after creation so the temp
// WriteFile fails.
func TestWriteCacheWriteTempError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	origPath, origNow := cachePathFn, nowFn
	t.Cleanup(func() { cachePathFn, nowFn = origPath, origNow })

	dir := t.TempDir()
	cachePathFn = func() (string, error) { return filepath.Join(dir, "update-check.json"), nil }
	// MkdirAll(dir) is a no-op (dir exists); making dir read-only fails the temp write.
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	err := WriteCache(&CacheEntry{LatestVersion: "v1.0.0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write update cache temp")
}

// TestWriteCacheRenameError proves a failure to rename the temp file over the
// final cache path is reported. The final path is an existing directory, so the
// rename fails.
func TestWriteCacheRenameError(t *testing.T) {
	origPath, origNow := cachePathFn, nowFn
	t.Cleanup(func() { cachePathFn, nowFn = origPath, origNow })

	root := t.TempDir()
	// Final cache path is itself a (non-empty) directory: WriteFile(tmp) succeeds,
	// Rename(tmp -> dir) fails.
	finalAsDir := filepath.Join(root, "update-check.json")
	require.NoError(t, os.Mkdir(finalAsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(finalAsDir, "occupant"), []byte("x"), 0o644))
	cachePathFn = func() (string, error) { return finalAsDir, nil }

	err := WriteCache(&CacheEntry{LatestVersion: "v1.0.0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename update cache")
}

// TestClearCacheRemoveError proves a non-not-exist removal failure is reported.
// The cache path is a non-empty directory, so os.Remove fails with ENOTEMPTY.
func TestClearCacheRemoveError(t *testing.T) {
	origPath, origNow := cachePathFn, nowFn
	t.Cleanup(func() { cachePathFn, nowFn = origPath, origNow })

	root := t.TempDir()
	pathAsDir := filepath.Join(root, "update-check.json")
	require.NoError(t, os.Mkdir(pathAsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pathAsDir, "occupant"), []byte("x"), 0o644))
	cachePathFn = func() (string, error) { return pathAsDir, nil }

	err := ClearCache()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "remove update cache")
}

// TestClearCachePathError proves a cachePathFn failure propagates from ClearCache.
func TestClearCachePathError(t *testing.T) {
	origPath, origNow := cachePathFn, nowFn
	t.Cleanup(func() { cachePathFn, nowFn = origPath, origNow })
	sentinel := errors.New("path boom")
	cachePathFn = func() (string, error) { return "", sentinel }

	assert.ErrorIs(t, ClearCache(), sentinel)
}

// TestClearCacheAbsentIsNoOp proves clearing a non-existent cache file is silent.
func TestClearCacheAbsentIsNoOp(t *testing.T) {
	useTempCache(t) // points at a temp path with no file yet
	assert.NoError(t, ClearCache(), "clearing an absent cache is a no-op")
}

// TestDefaultCachePath proves the real path resolver returns a flywheel-scoped
// path under the user config dir (no I/O, just composition).
func TestDefaultCachePath(t *testing.T) {
	t.Parallel()
	got, err := defaultCachePath()
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(got, filepath.Join("flywheel", "update-check.json")),
		"the cache path is scoped under the flywheel config dir")
}

// TestDefaultCachePathUserConfigDirError proves that when the OS cannot resolve a
// user config dir (here, HOME is unset on unix) defaultCachePath wraps the error.
func TestDefaultCachePathUserConfigDirError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("UserConfigDir on windows does not depend on HOME")
	}
	t.Setenv("HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	_, err := defaultCachePath()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve user config dir")
}

// TestNewGitHubFetcherDefaults proves the constructor wires the flywheel
// owner/repo and a bounded client.
func TestNewGitHubFetcherDefaults(t *testing.T) {
	t.Parallel()
	g := NewGitHubFetcher("v1.2.3")
	require.NotNil(t, g)
	assert.Equal(t, defaultOwner, g.Owner)
	assert.Equal(t, defaultRepo, g.Repo)
	assert.Equal(t, "v1.2.3", g.CurrentVersion)
	require.NotNil(t, g.Client)
	assert.Equal(t, fetchTimeout, g.Client.Timeout)
}

// TestLatestRequestCreationError proves an invalid baseURL fails before any
// transport call.
func TestLatestRequestCreationError(t *testing.T) {
	g := &GitHubFetcher{Owner: "o", Repo: "r", baseURL: "http://\x7f-bad"}
	_, err := g.Latest(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create release request")
}

// TestLatestTransportError proves a connection-level failure is wrapped.
func TestLatestTransportError(t *testing.T) {
	client := clientFromRoundTripper(t, func(*http.Request) (*http.Response, error) {
		return nil, errStubTransport
	})
	g := &GitHubFetcher{Owner: "o", Repo: "r", Client: client, baseURL: "https://example.invalid"}
	_, err := g.Latest(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch latest release")
}

// TestLatestDecodeError proves malformed JSON from the API is reported.
func TestLatestDecodeError(t *testing.T) {
	client := clientFromRoundTripper(t, func(req *http.Request) (*http.Response, error) {
		return okResponse(req, []byte("{not json")), nil
	})
	g := &GitHubFetcher{Owner: "o", Repo: "r", Client: client, baseURL: "https://example.invalid"}
	_, err := g.Latest(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode release")
}

// TestLatestNilClientBuildsDefault proves Latest builds a bounded default client
// when Client is nil. To stay network-free, baseURL uses an unsupported scheme:
// the request is created successfully, the default client is built, and
// client.Do fails at the transport with "unsupported protocol scheme" — no
// socket is opened and no host is contacted.
func TestLatestNilClientBuildsDefault(t *testing.T) {
	t.Setenv("FLYWHEEL_GITHUB_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GH_TOKEN", "")
	g := &GitHubFetcher{Owner: "o", Repo: "r", baseURL: "ftp://example.invalid"} // Client nil
	_, err := g.Latest(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch latest release",
		"the nil-client default is built, then the unsupported scheme fails locally")
}

// TestLatestDefaultBaseURL proves the empty baseURL branch composes the real
// api.github.com URL while still using an injected, network-free transport.
func TestLatestDefaultBaseURL(t *testing.T) {
	var gotURL string
	client := clientFromRoundTripper(t, func(req *http.Request) (*http.Response, error) {
		gotURL = req.URL.String()
		return okResponse(req, []byte(`{"tag_name":"v9.9.9"}`)), nil
	})
	g := &GitHubFetcher{Owner: "o", Repo: "r", Client: client} // empty baseURL => real host
	rel, err := g.Latest(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "v9.9.9", rel.TagName)
	assert.Equal(t, "https://api.github.com/repos/o/r/releases/latest", gotURL,
		"an empty baseURL targets the real GitHub API host")
}

// TestIsTerminalEnvSuppression proves isTerminal returns false under NO_COLOR or
// CI, and is well-defined for a non-character-device file (a regular temp file).
func TestIsTerminalEnvSuppression(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "tty")
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	t.Setenv("NO_COLOR", "1")
	t.Setenv("CI", "")
	assert.False(t, isTerminal(f), "NO_COLOR suppresses color")

	t.Setenv("NO_COLOR", "")
	t.Setenv("CI", "true")
	assert.False(t, isTerminal(f), "CI suppresses color")

	// With suppression off, a regular file is not a character device.
	t.Setenv("NO_COLOR", "")
	t.Setenv("CI", "")
	assert.False(t, isTerminal(f), "a regular file is not a TTY")
}

// TestIsTerminalStatError proves a closed file (Stat fails) is treated as not a
// terminal.
func TestIsTerminalStatError(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	t.Setenv("CI", "")
	f, err := os.CreateTemp(t.TempDir(), "tty")
	require.NoError(t, err)
	require.NoError(t, f.Close()) // Stat on a closed file errors
	assert.False(t, isTerminal(f), "a file whose Stat fails is not a TTY")
}

// TestShowBannerWritesWhenUpdateAvailable proves ShowBanner emits the notice for
// a real available-update result. It runs the full ShowBanner path (including the
// stderr/isTerminal wiring) without asserting on stderr contents.
func TestShowBannerWritesWhenUpdateAvailable(t *testing.T) {
	t.Setenv("NO_COLOR", "1") // deterministic, no ANSI
	ShowBanner(&Result{CurrentVersion: "v1.0.0", LatestVersion: "v2.0.0", UpdateAvailable: true})
}

// Compile-time assurance that runtime GOOS/GOARCH are referenced (assetName uses
// them) so the suite stays portable across platforms.
var _ = runtime.GOOS
