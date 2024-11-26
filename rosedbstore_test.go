// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

package rosedbstore_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/rosedbstore"
)

var keepOutput = flag.Bool("keep", false, "Keep test output after running")

func TestStore(t *testing.T) {
	dir, err := os.MkdirTemp("", "rosedbtest")
	if err != nil {
		t.Fatalf("Creating temp directory: %v", err)
	}
	path := filepath.Join(dir, "rose.db")
	t.Logf("Test store: %s", path)
	if !*keepOutput {
		defer os.RemoveAll(dir) // best effort cleanup
	}

	s, err := rosedbstore.Open(path, nil)
	if err != nil {
		t.Fatalf("Creating store at %q: %v", path, err)
	}
	storetest.Run(t, s)
	if err := s.Close(context.Background()); err != nil {
		t.Errorf("Closing store: %v", err)
	}
}
