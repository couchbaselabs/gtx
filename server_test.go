package gtx

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	everyone := map[Addr]*MemPeer{}
	a := NewMemPeer("a", everyone, make(chan MemMsg, 10))
	ms := NewMemStore()
	sc := NewServerController(a, ms)
	if sc == nil {
		t.Errorf("expected sc")
	}
}

func TestBasicAbort(t *testing.T) {
	everyone := map[Addr]*MemPeer{}
	a := NewMemPeer("a", everyone, make(chan MemMsg, 10))
	ms := NewMemStore()
	sc := NewServerController(a, ms)
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

func TestBasicCommit(t *testing.T) {
	everyone := map[Addr]*MemPeer{}
	a := NewMemPeer("a", everyone, make(chan MemMsg, 10))
	ms := NewMemStore()
	sc := NewServerController(a, ms)
	if sc == nil {
		t.Errorf("expected sc")
	}
	t0 := NewTransaction(sc, 0)
	if t0.Set("x", []byte("xxx")) != nil {
		t.Errorf("expected set to work")
	}
	if t0.Commit() != nil {
		t.Errorf("expected commit to work")
	}
	sentOk, sentErr := a.SendAllMessages(sc)
	if sentOk != 1 || sentErr != 0 {
		t.Errorf("unexpected sentOk: %v, sentErr: %v", sentOk, sentErr)
	}

	t1 := NewTransaction(sc, 1)
	v, err := t1.Get("x")
	if err != nil || string(v) != "xxx" {
		fmt.Printf("ms.good: %#v\n", ms.good)
		t.Errorf("expected get to give xxx, v: %v, err: %v", v, err)
	}

	t2 := NewTransaction(sc, 2)
	if t2.Set("x", []byte("xxxx")) != nil {
		t.Errorf("expected set to work")
	}
	if t2.Commit() != nil {
		t.Errorf("expected commit to work")
	}
	sentOk, sentErr = a.SendAllMessages(sc)
	if sentOk != 1 || sentErr != 0 {
		t.Errorf("unexpected sentOk: %v, sentErr: %v", sentOk, sentErr)
	}

	t3 := NewTransaction(sc, 3)
	v, err = t3.Get("x")
	if err != nil || string(v) != "xxxx" {
		t.Errorf("expected get to give xxxx, v: %v, err: %v", v, err)
	}

	t4 := NewTransaction(sc, 4)
	if t4.Del("x") != nil {
		t.Errorf("expected del to work")
	}
	if t4.Commit() != nil {
		t.Errorf("expected commit to work")
	}
	sentOk, sentErr = a.SendAllMessages(sc)
	if sentOk != 1 || sentErr != 0 {
		t.Errorf("unexpected sentOk: %v, sentErr: %v", sentOk, sentErr)
	}

	t5 := NewTransaction(sc, 5)
	v, err = t5.Get("x")
	if err != nil || v != nil {
		t.Errorf("expected t3.Get to give nil, v: %v, err: %v", v, err)
	}
}
