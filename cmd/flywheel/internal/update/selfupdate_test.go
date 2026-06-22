package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTarGz builds a gzip-compressed tar holding a single regular file.
func makeTarGz(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: name, Mode: 0o755, Size: int64(len(content)), Typeflag: tar.TypeReg,
	}))
	_, err := tw.Write(content)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	return buf.Bytes()
}

// assetName is the goreleaser archive name for this OS/arch.
func assetName() string {
	return fmt.Sprintf("go-flywheel_2.0.0_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
}

// releaseServer serves the archive at /asset and the given checksums at
// /checksums.txt, returning the httptest server and a matching Release.
func releaseServer(t *testing.T, archive []byte, checksums string) (*httptest.Server, *Release) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/asset":
			_, _ = w.Write(archive)
		case "/checksums.txt":
			_, _ = w.Write([]byte(checksums))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	rel := &Release{TagName: "v2.0.0", Assets: []Asset{
		{Name: assetName(), URL: srv.URL + "/asset"},
		{Name: "go-flywheel_2.0.0_checksums.txt", URL: srv.URL + "/checksums.txt"},
	}}
	return srv, rel
}

func TestSelfUpdateReplacesBinaryOnGoodChecksum(t *testing.T) {
	useTempCache(t) // keep ClearCache hermetic
	content := []byte("new-flywheel-binary-bytes")
	archive := makeTarGz(t, "flywheel", content)
	sum := sha256.Sum256(archive)
	checksums := fmt.Sprintf("%s  %s\n", hex.EncodeToString(sum[:]), assetName())

	srv, rel := releaseServer(t, archive, checksums)

	target := filepath.Join(t.TempDir(), "flywheel")
	require.NoError(t, os.WriteFile(target, []byte("old-binary"), 0o755))

	latest, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		TargetPath: target, Client: srv.Client(),
	})
	require.NoError(t, err)
	assert.True(t, updated)
	assert.Equal(t, "v2.0.0", latest)

	got, err := os.ReadFile(target) //nolint:gosec // test-controlled temp path
	require.NoError(t, err)
	assert.Equal(t, content, got, "the target binary is replaced with the verified release content")

	info, err := os.Stat(target)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode().Perm()&0o100, "the installed binary stays executable")
}

func TestSelfUpdateRejectsChecksumMismatch(t *testing.T) {
	useTempCache(t)
	archive := makeTarGz(t, "flywheel", []byte("real content"))
	badChecksums := fmt.Sprintf("%s  %s\n", strings.Repeat("0", 64), assetName())

	srv, rel := releaseServer(t, archive, badChecksums)

	target := filepath.Join(t.TempDir(), "flywheel")
	require.NoError(t, os.WriteFile(target, []byte("old-binary"), 0o755))

	_, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel}, Options{
		TargetPath: target, Client: srv.Client(),
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, errChecksumMismatch)
	assert.False(t, updated)

	got, err := os.ReadFile(target) //nolint:gosec // test-controlled temp path
	require.NoError(t, err)
	assert.Equal(t, []byte("old-binary"), got, "a checksum mismatch leaves the target untouched")
}

func TestSelfUpdateSkipsWhenNotNewer(t *testing.T) {
	useTempCache(t)
	latest, updated, err := SelfUpdate(
		context.Background(), "v2.0.0",
		&fakeFetcher{rel: &Release{TagName: "v2.0.0"}},
		Options{TargetPath: filepath.Join(t.TempDir(), "flywheel")},
	)
	require.NoError(t, err)
	assert.False(t, updated, "an up-to-date binary is not replaced")
	assert.Equal(t, "v2.0.0", latest)
}

func TestSelfUpdateForceInstallsSameVersion(t *testing.T) {
	useTempCache(t)
	content := []byte("forced-binary")
	archive := makeTarGz(t, "flywheel", content)
	sum := sha256.Sum256(archive)
	checksums := fmt.Sprintf("%s  %s\n", hex.EncodeToString(sum[:]), assetName())
	srv, rel := releaseServer(t, archive, checksums)

	target := filepath.Join(t.TempDir(), "flywheel")
	require.NoError(t, os.WriteFile(target, []byte("old"), 0o755))

	// Current == latest, but Force installs anyway.
	_, updated, err := SelfUpdate(context.Background(), "v2.0.0", &fakeFetcher{rel: rel}, Options{
		Force: true, TargetPath: target, Client: srv.Client(),
	})
	require.NoError(t, err)
	assert.True(t, updated)
	got, _ := os.ReadFile(target) //nolint:gosec // test-controlled temp path
	assert.Equal(t, content, got)
}

func TestSelectAssetErrors(t *testing.T) {
	t.Parallel()
	_, _, err := selectAsset(&Release{Assets: []Asset{{Name: "go-flywheel_2.0.0_checksums.txt", URL: "u"}}})
	assert.ErrorIs(t, err, errNoAsset, "no matching os/arch asset is an error")

	_, _, err = selectAsset(&Release{Assets: []Asset{{Name: assetName(), URL: "u"}}})
	assert.ErrorIs(t, err, errNoChecksums, "a release without checksums.txt is an error")
}

func TestExtractBinaryNotFound(t *testing.T) {
	t.Parallel()
	archive := makeTarGz(t, "not-flywheel", []byte("x"))
	_, err := extractBinary(archive, "flywheel", t.TempDir())
	assert.ErrorIs(t, err, errBinaryNotFound)
}

func TestSelfUpdateErrorsWhenAssetMissing(t *testing.T) {
	useTempCache(t)
	// A newer release with no asset for this os/arch surfaces errNoAsset.
	rel := &Release{TagName: "v2.0.0", Assets: []Asset{{Name: "go-flywheel_2.0.0_checksums.txt", URL: "u"}}}
	_, updated, err := SelfUpdate(context.Background(), "v1.0.0", &fakeFetcher{rel: rel},
		Options{TargetPath: filepath.Join(t.TempDir(), "flywheel")})
	assert.ErrorIs(t, err, errNoAsset)
	assert.False(t, updated)
}

func TestVerifyChecksumNotListed(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("a", 64) + "  some-other-file.tar.gz\n"))
	}))
	defer srv.Close()

	err := verifyChecksum(context.Background(), srv.Client(), []byte("data"), assetName(), srv.URL)
	assert.ErrorIs(t, err, errChecksumNotFound, "an asset absent from checksums.txt is rejected")
}

func TestDownloadRejectsNon200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := download(context.Background(), srv.Client(), srv.URL, 1024)
	require.Error(t, err)
}

func TestDownloadRejectsOversized(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, 64))
	}))
	defer srv.Close()

	_, err := download(context.Background(), srv.Client(), srv.URL, 8) // body exceeds the cap
	assert.ErrorIs(t, err, errAssetTooLarge)
}

func TestSafeJoinRejectsTraversal(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	_, err := safeJoin(dir, "../escape")
	assert.ErrorIs(t, err, errPathTraversal)

	_, err = safeJoin(dir, "/etc/passwd")
	assert.ErrorIs(t, err, errPathTraversal)

	got, err := safeJoin(dir, "flywheel")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "flywheel"), got)
}
