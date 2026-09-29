package stdlib

import "testing"

func TestNewTinyLFU_ConstructorsAndHashRequirement(t *testing.T) {
	lfu, err := NewTinyLFU(TinyLFUOptions[string]{Depth: 4, Width: 64, AgingEvery: 0, AdmitOnEqual: true, Hash: Hash64String})
	if err != nil || lfu == nil {
		t.Fatalf("unexpected: %v %v", lfu, err)
	}

	opt := TinyLFUOptions[string]{Depth: 4, Width: 64, AgingEvery: 0, AdmitOnEqual: false, Hash: Hash64String}
	lfu2, err := NewTinyLFU(opt)
	if err != nil || lfu2 == nil {
		t.Fatalf("unexpected options ctor: %v %v", lfu2, err)
	}

	// Hash required (panic)
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on nil hash")
		}
	}()
	_, _ = NewTinyLFU(TinyLFUOptions[string]{Depth: 4, Width: 64})
}

func TestNewTinyLFU_PropagatesSketchError(t *testing.T) {
	// Invalid depth -> underlying CMS fails; ensure error is returned
	_, err := NewTinyLFU(TinyLFUOptions[string]{Depth: 0, Width: 64, AgingEvery: 0, Hash: Hash64String})
	if err == nil {
		t.Fatalf("expected error for invalid depth, got nil")
	}

	// Invalid width via options
	_, err = NewTinyLFU(TinyLFUOptions[string]{Depth: 4, Width: 8, AgingEvery: 0, AdmitOnEqual: true, Hash: Hash64String})
	if err == nil {
		t.Fatalf("expected error for invalid width, got nil")
	}
}

func TestTinyLFU_RecordEstimateVictimFreq(t *testing.T) {
	lfu, _ := NewTinyLFU(TinyLFUOptions[string]{Depth: 4, Width: 64, AgingEvery: 0, AdmitOnEqual: true, Hash: Hash64String})
	// Start zeros
	if got := lfu.Estimate("a"); got != 0 {
		t.Fatalf("estimate a=%d, want 0", got)
	}
	if got := lfu.VictimFreq("a"); got != 0 {
		t.Fatalf("victim freq a=%d, want 0", got)
	}

	// Record hits and check monotonicity
	lfu.Record("a")
	lfu.Record("a")
	if got := lfu.Estimate("a"); got < 2 {
		t.Fatalf("estimate a < 2: %d", got)
	}
	if got := lfu.VictimFreq("a"); got < 2 {
		t.Fatalf("victim freq a < 2: %d", got)
	}
}

func TestTinyLFU_RecordNAndNoopZero(t *testing.T) {
	lfu, _ := NewTinyLFU(TinyLFUOptions[string]{Depth: 4, Width: 64, AgingEvery: 0, AdmitOnEqual: true, Hash: Hash64String})
	// n=0 must be a no-op
	lfu.RecordN("x", 0)
	if got := lfu.Estimate("x"); got != 0 {
		t.Fatalf("estimate x after RecordN(0) = %d, want 0", got)
	}
	// n>0 increases estimate
	lfu.RecordN("x", 3)
	if got := lfu.Estimate("x"); got < 3 {
		t.Fatalf("estimate x after RecordN(3) = %d, want >=3", got)
	}
}

func TestTinyLFU_AdmitPolicy_WithAdmitOnEqualTrue(t *testing.T) {
	lfu, _ := NewTinyLFU(TinyLFUOptions[string]{Depth: 4, Width: 64, AgingEvery: 0, AdmitOnEqual: true, Hash: Hash64String})
	// Make a hotter than b
	lfu.Record("a")
	lfu.Record("a")
	lfu.Record("b")
	if !lfu.ShouldAdmit("a", "b") {
		t.Fatalf("expected admit a over b")
	}
	if lfu.ShouldAdmit("b", "a") {
		t.Fatalf("expected reject b vs a")
	}
	// Tie should admit (>=)
	lfu.Reset()
	// both zero -> tie
	if !lfu.ShouldAdmit("x", "y") {
		t.Fatalf("expected admit on tie when AdmitOnEqual=true")
	}
	// Against explicit frequency
	lfu.Record("z")
	vf := lfu.VictimFreq("z")
	if !lfu.ShouldAdmitAgainst("z", vf) {
		t.Fatalf("expected admit when equal via ShouldAdmitAgainst")
	}
}

func TestTinyLFU_AdmitPolicy_WithAdmitOnEqualFalse(t *testing.T) {
	lfu, _ := NewTinyLFU(TinyLFUOptions[string]{Depth: 4, Width: 64, AgingEvery: 0, AdmitOnEqual: false, Hash: Hash64String})
	// Tie should reject when AdmitOnEqual=false
	if lfu.ShouldAdmit("x", "y") {
		t.Fatalf("expected reject on tie when AdmitOnEqual=false")
	}
	// When incoming hotter than victim, should admit
	lfu.Record("hot")
	lfu.Record("hot")
	lfu.Record("cold")
	if !lfu.ShouldAdmit("hot", "cold") {
		t.Fatalf("expected admit hot over cold")
	}
	// Against explicit frequency
	vf := lfu.VictimFreq("cold")
	if !lfu.ShouldAdmitAgainst("hot", vf-1) {
		t.Fatalf("expected admit with fi > victimFreq")
	}
	if lfu.ShouldAdmitAgainst("hot", vf+1000) {
		t.Fatalf("expected reject when fi < victimFreq")
	}
}

func TestTinyLFU_AgeAndResetForwarders(t *testing.T) {
	lfu, _ := NewTinyLFU(TinyLFUOptions[string]{Depth: 4, Width: 64, AgingEvery: 0, AdmitOnEqual: true, Hash: Hash64String})
	for i := 0; i < 8; i++ {
		lfu.Record("k")
	}
	before := lfu.Estimate("k")
	if before == 0 {
		t.Fatalf("expect >0 before age/reset")
	}
	lfu.Age()
	after := lfu.Estimate("k")
	if after > before {
		t.Fatalf("age should not increase estimate: before=%d after=%d", before, after)
	}
	lfu.Reset()
	if got := lfu.Estimate("k"); got != 0 {
		t.Fatalf("expect 0 after reset, got %d", got)
	}
}
