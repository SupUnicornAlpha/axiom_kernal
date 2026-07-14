package axiom

import "testing"

func TestBuilderDefaultsAndDigest(t *testing.T) {
	spec := NewRunSpec("run", "name").Lease("tool").Build()
	if spec.SchemaVersion != 1 || spec.Budget.MaxSteps != 128 || len(spec.CapabilityLeases) != 1 {
		t.Fatalf("unexpected spec: %#v", spec)
	}
	digest, err := spec.Digest()
	if err != nil || len(digest) != 64 {
		t.Fatalf("digest=%q err=%v", digest, err)
	}
}
