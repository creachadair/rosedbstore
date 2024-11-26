// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

// Package rosedbstore implements the [blob.Store] interfaace using [rosedb].
//
// [rosedb]: https://github.com/rosedblabs/rosedb
package rosedbstore

import (
	"context"
	"errors"

	"github.com/creachadair/ffs/blob"
	"github.com/rosedblabs/rosedb/v2"
)

// Store implements the [blob.Store] interface using a rosedb database.
type Store struct {
	db *rosedb.DB
}

// Opener constructs a [Store] from an address comprising a path.
func Opener(_ context.Context, addr string) (blob.Store, error) { return Open(addr, nil) }

// Open creates a [Store] by opening the rosedb database at path.
func Open(path string, opts *Options) (*Store, error) {
	db, err := rosedb.Open(rosedb.Options{
		DirPath:           path,
		SegmentSize:       1 << 30,
		AutoMergeCronExpr: "* * * * *", // once a minute
	})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Get implements part of the [blob.Store] interface.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, blob.ErrKeyNotFound
	}
	data, err := s.db.Get([]byte(key))
	if errors.Is(err, rosedb.ErrKeyNotFound) {
		return nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

// Put implements part of the [blob.Store] interface.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
	if opts.Key == "" {
		return blob.ErrKeyNotFound
	}
	bkey := []byte(opts.Key)
	if !opts.Replace {
		ok, err := s.db.Exist(bkey)
		if err != nil {
			return err
		} else if ok {
			return blob.KeyExists(opts.Key)
		}
	}
	return s.db.Put(bkey, opts.Data)
}

// Delete implements part of the [blob.Store] interface.
func (s *Store) Delete(ctx context.Context, key string) error {
	if key == "" {
		return blob.ErrKeyNotFound
	}
	bkey := []byte(key)
	ok, err := s.db.Exist(bkey)
	if err != nil {
		return err
	} else if !ok {
		return blob.KeyNotFound(key)
	}
	return s.db.Delete(bkey)
}

// List implements part of the [blob.Store] interface.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
	var ferr error
	s.db.AscendGreaterOrEqual([]byte(start), func(key, _ []byte) (bool, error) {
		if err := f(string(key)); errors.Is(err, blob.ErrStopListing) {
			return false, nil
		} else if err != nil {
			ferr = err
			return false, nil
		}
		return true, nil
	})
	return ferr
}

// Len implements part of the [blob.Store] interface.
func (s *Store) Len(ctx context.Context) (int64, error) {
	return int64(s.db.Stat().KeysNum), nil
}

// Close implements part of the [blob.Store] interface.
func (s *Store) Close(_ context.Context) error {
	merr := s.db.Merge(false)
	if errors.Is(merr, rosedb.ErrDBClosed) {
		merr = nil
	}
	cerr := s.db.Close()
	return errors.Join(merr, cerr)
}

// Options provide options for opening a rosedb database.
// A nil *Options is ready for use and provides default values.
type Options struct{}
