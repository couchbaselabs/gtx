package cbtx

import (
	"testing"
)

func TestBasic(t *testing.T) {
	sc := NewServerController(nil)
	if sc == nil {
		t.Errorf("expected sc")
	}
}
