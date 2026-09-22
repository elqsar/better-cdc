package subject

import (
	"net/url"
	"testing"
)

func TestTokenIsReversibleAndCollisionFree(t *testing.T) {
	seen := map[string]bool{}
	for _, input := range []string{"normal", "a.b", "a*b", "a>b", "a b", "a_b", "%2E", "žluťoučký", "MixedCase", "x\ty"} {
		encoded := Token(input)
		if seen[encoded] {
			t.Fatal("collision")
		}
		seen[encoded] = true
		decoded, err := url.PathUnescape(encoded)
		if err != nil || decoded != input {
			t.Fatalf("roundtrip %q: %q", input, encoded)
		}
	}
}
