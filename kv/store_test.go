package kv

import "testing"

func TestStorePutGet(t *testing.T) {
	s := NewStore()
	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected missing key")
	}
	s.Put("a", "1")
	s.Put("a", "2")
	v, ok := s.Get("a")
	if !ok || v != "2" {
		t.Fatalf("got %q %v, want 2 true", v, ok)
	}
	if s.Len() != 1 {
		t.Fatalf("len = %d, want 1", s.Len())
	}
}

func TestStoreSnapshotRestore(t *testing.T) {
	s := NewStore()
	s.Put("color", "blue")
	s.Put("city", "paris")
	blob, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s2 := NewStore()
	if err := s2.Restore(blob); err != nil {
		t.Fatal(err)
	}
	if v, ok := s2.Get("color"); !ok || v != "blue" {
		t.Fatalf("color = %q %v", v, ok)
	}
	if s2.Len() != 2 {
		t.Fatalf("len = %d", s2.Len())
	}
	if err := s2.Restore(nil); err != nil {
		t.Fatal(err)
	}
	if s2.Len() != 0 {
		t.Fatalf("restore empty should clear, len=%d", s2.Len())
	}
}
