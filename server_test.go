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
	a.sc = sc

	t0 := NewTransaction(sc, 10)
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
	a.sc = sc

	t0 := NewTransaction(sc, 10)
	if t0.Set("x", []byte("xxx")) != nil {
		t.Errorf("expected set to work")
	}
	if t0.Commit(false) != nil {
		t.Errorf("expected commit to work")
	}
	sentOk, sentErr := a.SendMessages(-1)
	if sentOk != 1 || sentErr != 0 {
		t.Errorf("unexpected sentOk: %v, sentErr: %v", sentOk, sentErr)
	}

	t1 := NewTransaction(sc, 11)
	v, err := t1.Get("x")
	if err != nil || string(v) != "xxx" {
		fmt.Printf("ms.stable: %#v\n", ms.stable)
		t.Errorf("expected get to give xxx, v: %v, err: %v", v, err)
	}

	t2 := NewTransaction(sc, 12)
	if t2.Set("x", []byte("xxxx")) != nil {
		t.Errorf("expected set to work")
	}
	if t2.Commit(false) != nil {
		t.Errorf("expected commit to work")
	}
	sentOk, sentErr = a.SendMessages(-1)
	if sentOk != 1 || sentErr != 0 {
		t.Errorf("unexpected sentOk: %v, sentErr: %v", sentOk, sentErr)
	}

	t3 := NewTransaction(sc, 13)
	v, err = t3.Get("x")
	if err != nil || string(v) != "xxxx" {
		t.Errorf("expected get to give xxxx, v: %v, err: %v", v, err)
	}

	t4 := NewTransaction(sc, 14)
	if t4.Del("x") != nil {
		t.Errorf("expected del to work")
	}
	if t4.Commit(false) != nil {
		t.Errorf("expected commit to work")
	}
	sentOk, sentErr = a.SendMessages(-1)
	if sentOk != 1 || sentErr != 0 {
		t.Errorf("unexpected sentOk: %v, sentErr: %v", sentOk, sentErr)
	}

	t5 := NewTransaction(sc, 15)
	v, err = t5.Get("x")
	if err != nil || v != nil {
		t.Errorf("expected t3.Get to give nil, v: %v, err: %v", v, err)
	}
}

func TestCommitErrorIfConcurrent(t *testing.T) {
	everyone := map[Addr]*MemPeer{}
	a := NewMemPeer("a", everyone, make(chan MemMsg, 10))
	ms := NewMemStore()
	sc := NewServerController(a, ms)
	if sc == nil {
		t.Errorf("expected sc")
	}
	a.sc = sc

	t0 := NewTransaction(sc, 10)
	t0.Set("x", []byte("xxx"))
	if t0.Commit(true) != nil {
		t.Errorf("expected commit true to work")
	}
	a.SendMessages(-1)

	t1 := NewTransaction(sc, 11)
	v, err := t1.Get("x")
	if err != nil || string(v) != "xxx" {
		t.Errorf("expected Get(x) to work, v: %v, err: %v", v, err)
	}
	t1.Set("x", []byte("xxx1"))
	v, err = t1.Get("x")
	if err != nil || string(v) != "xxx1" {
		t.Errorf("expected Get(x) to work, v: %v, err: %v", v, err)
	}

	t2 := NewTransaction(sc, 12)
	v, err = t2.Get("x")
	if err != nil || string(v) != "xxx" {
		t.Errorf("expected Get(x) to work, v: %v, err: %v", v, err)
	}
	t2.Set("x", []byte("xxx2"))
	v, err = t2.Get("x")
	if err != nil || string(v) != "xxx2" {
		t.Errorf("expected Get(x) to work, v: %v, err: %v", v, err)
	}

	if t1.Commit(true) != nil {
		t.Errorf("expected commit t1 true to work")
	}
	a.SendMessages(-1)

	v, err = t2.Get("x")
	if err != nil || string(v) != "xxx2" {
		t.Errorf("expected Get(x) to work, v: %v, err: %v", v, err)
	}
	if t2.Commit(true) == nil {
		t.Errorf("expected commit t2 true to fail")
	}
}

func TestTwoKey(t *testing.T) {
	testTwoKey(t, -1, 0)
}

func TestTwoKeyWithDuplicateMessages(t *testing.T) {
	testTwoKey(t, 10, 50)
}

func testTwoKey(t *testing.T, maxSend int, dupePct int) {
	everyone := map[Addr]*MemPeer{}
	a := NewMemPeer("a", everyone, make(chan MemMsg, 10))
	ms := NewMemStore()
	sc := NewServerController(a, ms)
	if sc == nil {
		t.Errorf("expected sc")
	}
	a.sc = sc

	tx := NewTransaction(sc, 10)
	tx.Set("x", []byte("xx"))
	tx.Set("y", []byte("yy"))
	if tx.Commit(true) != nil {
		t.Errorf("expected commit to work")
	}
	sentOk, sentErr := a.SendDuplicateMessages(maxSend, dupePct)
	if dupePct == 0 && (sentOk != 4 || sentErr != 0) {
		t.Errorf("unexpected sentOk: %v, sentErr: %v", sentOk, sentErr)
	}

	tx = NewTransaction(sc, 11)
	v, err := tx.Get("x")
	if err != nil || string(v) != "xx" {
		t.Errorf("expected Get to work, v: %v, err: %v", v, err)
	}
	v, err = tx.Get("y")
	if err != nil || string(v) != "yy" {
		t.Errorf("expected Get to work, v: %v, err: %v", v, err)
	}
	tx.Set("x", []byte("xxx"))
	tx.Set("y", []byte("yyy"))
	if tx.Commit(true) != nil {
		t.Errorf("expected commit to work")
	}
	sentOk, sentErr = a.SendDuplicateMessages(maxSend, dupePct)
	if dupePct == 0 && (sentOk != 4 || sentErr != 0) {
		t.Errorf("unexpected sentOk: %v, sentErr: %v", sentOk, sentErr)
	}

	tx = NewTransaction(sc, 12)
	v, err = tx.Get("x")
	if err != nil || string(v) != "xxx" {
		t.Errorf("expected Get to work, v: %v, err: %v", v, err)
	}
	v, err = tx.Get("y")
	if err != nil || string(v) != "yyy" {
		t.Errorf("expected Get to work, v: %v, err: %v", v, err)
	}
	tx.Set("x", []byte("xxxx"))
	if tx.Commit(true) != nil {
		t.Errorf("expected commit to work")
	}
	sentOk, sentErr = a.SendDuplicateMessages(maxSend, dupePct)
	if dupePct == 0 && (sentOk != 1 || sentErr != 0) {
		t.Errorf("unexpected sentOk: %v, sentErr: %v", sentOk, sentErr)
	}

	tx = NewTransaction(sc, 13)
	v, err = tx.Get("x")
	if err != nil || string(v) != "xxxx" {
		t.Errorf("expected Get to work, v: %v, err: %v", v, err)
	}
	v, err = tx.Get("y")
	if err != nil || string(v) != "yyy" {
		t.Errorf("expected Get to work, v: %v, err: %v", v, err)
	}
}
