package fingerprint

import "testing"

func TestCompute_Deterministic(t *testing.T) {
	labels := map[string]string{"job": "api", "instance": "localhost:9090"}
	a := Compute(labels)
	b := Compute(labels)
	if a != b {
		t.Errorf("non-deterministic: %x != %x", a, b)
	}
}

func TestCompute_OrderIndependent(t *testing.T) {
	a := Compute(map[string]string{"a": "1", "b": "2", "c": "3"})
	b := Compute(map[string]string{"c": "3", "a": "1", "b": "2"})
	if a != b {
		t.Errorf("order-dependent: %x != %x", a, b)
	}
}

func TestCompute_DifferentLabels_DifferentHash(t *testing.T) {
	a := Compute(map[string]string{"job": "api"})
	b := Compute(map[string]string{"job": "web"})
	if a == b {
		t.Errorf("collision: both = %x", a)
	}
}

func TestCompute_EmptyLabels(t *testing.T) {
	fp := Compute(map[string]string{})
	if fp == [16]byte{} {
		t.Error("empty labels should produce non-zero fingerprint (xxh3 of empty input is non-zero)")
	}
}

func TestCompute_KeyValueSeparation(t *testing.T) {
	// "a"="bc" vs "ab"="c" must differ
	a := Compute(map[string]string{"a": "bc"})
	b := Compute(map[string]string{"ab": "c"})
	if a == b {
		t.Errorf("key-value boundary collision: both = %x", a)
	}
}

func TestCompute_IncludesName(t *testing.T) {
	// __name__ participates in the fingerprint
	a := Compute(map[string]string{"__name__": "foo", "job": "api"})
	b := Compute(map[string]string{"__name__": "bar", "job": "api"})
	if a == b {
		t.Errorf("__name__ not included in fingerprint: both = %x", a)
	}
}
