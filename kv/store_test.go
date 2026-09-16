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
