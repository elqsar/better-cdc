package parser

import "testing"

func TestSpillDirectoriesAreSourceScoped(t *testing.T) {
	base := t.TempDir()
	a, releaseA, err := PrepareSpillDir(base, "a", "same_slot")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseA()
	b, releaseB, err := PrepareSpillDir(base, "b", "same_slot")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseB()
	if a == b {
		t.Fatal("sources shared spill directory")
	}
	if _, release, err := PrepareSpillDir(base, "a", "same_slot"); err == nil {
		release()
		t.Fatal("duplicate owner accepted")
	}
}
