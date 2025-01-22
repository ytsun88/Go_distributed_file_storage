package main

import (
	"bytes"
	"io"
	"testing"
)

func TestPathTransformFunc(t *testing.T) {
	key := "momsbestpicture"
	path := CASPathTransformFunc(key)
	expectedFilename := "6804429f74181a63c50c3d81d733a12f14a353ff"
	expectedPathName := "68044/29f74/181a6/3c50c/3d81d/733a1/2f14a/353ff"
	if path.Filename != expectedFilename {
		t.Errorf("have %s want %s\n", path.Filename, expectedFilename)
	}
	if path.PathName != expectedPathName {
		t.Errorf("have %s want %s\n", path.PathName, expectedPathName)
	}
}

func TestStore(t *testing.T) {
	opts := StoreOpts {
		PathTransformFunc: CASPathTransformFunc,
	}
	s := NewStore(opts)
	id := generateID()
	defer teardown(t, s)

	key := "momsspecials"
	data := []byte("some jpg bytes")
	if _, err := s.Write(id, key, bytes.NewReader(data)); err != nil {
		t.Error(err)
	}
	_, r, err := s.Read(id, key)
	if err != nil {
		t.Error(err)
	}

	b, _ := io.ReadAll(r)
	if string(b) != string(data) {
		t.Errorf("have %s want %s\n", b, data)
	}
	if ok := s.Has(id, key); !ok {
		t.Errorf("expected to have %s\n", key)
	}
	if err := s.Delete(id, key); err != nil {
		t.Error(err)
	}
	if ok := s.Has(id, key); ok {
		t.Errorf("expected not to have %s\n", key)
	}
}

func teardown(t *testing.T, s *Store) {
	if err := s.Clear(); err != nil {
		t.Error(err)
	}
}