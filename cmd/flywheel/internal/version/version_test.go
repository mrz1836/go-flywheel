package version

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b string
		want int
	}{
		{"v1.2.3", "v1.2.3", 0},
		{"v1.2.4", "v1.2.3", 1},
		{"v1.2.3", "v1.2.4", -1},
		{"v2.0.0", "v1.9.9", 1},
		{"1.2.3", "v1.2.3", 0},      // leading v is ignored
		{"v1.2.3-rc1", "v1.2.3", 0}, // pre-release suffix dropped for numeric compare
		{"v1.2.3+dirty", "v1.2.3", 0},
		{"dev", "v1.0.0", -1}, // dev sorts older than any release
		{"v1.0.0", "dev", 1},
		{"dev", "dev", 0},
		{"", "v1.0.0", -1},
		{"abc1234", "v1.0.0", -1}, // a commit hash sorts older than a release
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, Compare(c.a, c.b), "Compare(%q, %q)", c.a, c.b)
	}
}

func TestIsNewer(t *testing.T) {
	t.Parallel()
	assert.True(t, IsNewer("v1.0.0", "v1.0.1"))
	assert.False(t, IsNewer("v1.0.1", "v1.0.0"))
	assert.False(t, IsNewer("v1.0.0", "v1.0.0"))
	assert.True(t, IsNewer("dev", "v0.0.1"), "any release is newer than a dev build")
	assert.False(t, IsNewer("v1.0.0", "dev"))
}
