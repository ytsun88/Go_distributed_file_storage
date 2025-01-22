package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

const defaultRootFolderName = "db" 

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])
	blocksize := 5
	sliceLen := len(hashStr) / blocksize
	paths := make([]string, sliceLen)
	for i := 0; i < sliceLen; i++ {
		from, to := i * blocksize, (i + 1) * blocksize
		paths[i] = hashStr[from: to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	PathName	string
	Filename	string
}

func (p *PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.Filename)
}

type StoreOpts struct {
	Root				string
	PathTransformFunc	PathTransformFunc
}
 
var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		Filename: key,
	}
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}
	return &Store{
		StoreOpts: opts,
	}
}

func (s *Store) Has(id string, key string) bool {
	pathKey := s.PathTransformFunc(key)
	_, err := os.Stat(s.Root + "/" + id + "/" + pathKey.FullPath())
	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

func (s *Store) Delete(id string, key string) error {
	pathKey := s.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.Filename)
	} ()

	return os.RemoveAll(s.Root + "/" + id + "/" + pathKey.FullPath())
}

func (s *Store) Read(id string, key string) (int64, io.Reader, error) {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := s.Root + "/" + id + "/" + pathKey.FullPath()
	
	f, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return 0, nil, err
	}

	return fi.Size(), f, nil
}

func (s *Store) WriteDecrypt(id string, encKey []byte, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, err
	}

	n, err := copyDecrypt(encKey, r, f)
	if err != nil {
		return 0, err
	}

	return int64(n), nil
}

func (s *Store) openFileForWriting(id string, key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)

	if err := os.MkdirAll(s.Root + "/" + id + "/" + pathKey.PathName, os.ModePerm); err != nil {
		return nil, err
	}

	fullPath := s.Root + "/" + id + "/" + pathKey.FullPath()

	return os.Create(fullPath)
}

func (s *Store) Write(id string, key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWriting(id, key)
	if err != nil {
		return 0, nil
	}

	return io.Copy(f, r)
}