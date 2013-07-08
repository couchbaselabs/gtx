package cbtx

import (
	"testing"
)

func TestNew(t *testing.T) {
	everyone := map[Addr]*MemPeer{}
	everyone["a"] = &MemPeer{everyone}
	ms := NewMemStore()
	sc := NewServerController(everyone["a"], ms)
	if sc == nil {
		t.Errorf("expected sc")
	}
}

func TestBasicAbort(t *testing.T) {
	everyone := map[Addr]*MemPeer{}
	everyone["a"] = &MemPeer{everyone}
	ms := NewMemStore()
	sc := NewServerController(everyone["a"], ms)
	if sc == nil {
		t.Errorf("expected sc")
	}
	t0 := NewTransaction(sc, 0)
	if t0 == nil {
		t.Errorf("expected t0")
	}
	if t0.Set("x", []byte("xxx")) != nil {
		t.Errorf("expected t0.Set to work")
	}
	v, err := t0.Get("x")
	if err != nil || string(v) != "xxx" {
		t.Errorf("expected t0.Get to give xxx")
	}
	if t0.Del("x") != nil {
		t.Errorf("expected t0.Del to work")
	}
	v, err = t0.Get("x")
	if err != nil || v != nil {
		t.Errorf("expected t0.Get to give nil")
	}
	if t0.Abort() != nil {
		t.Errorf("expected abort to work")
	}
}
