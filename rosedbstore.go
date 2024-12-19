// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

// Package rosedbstore implements the [blob.StoreCloser] interface on [rosedb].
//
// [rosedb]: https://github.com/rosedblabs/rosedb
package rosedbstore

import (
	"bytes"
	"context"
	"errors"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/creachadair/ffs/storage/monitor"
	"github.com/rosedblabs/rosedb/v2"
)

// Opener constructs a [KV] from an address comprising a path.
func Opener(_ context.Context, addr string) (blob.StoreCloser, error) {
	return Open(addr, nil)
}

type Store struct {
	*monitor.M[*rosedb.DB, KV]
}

// Close implements part of the [blob.KV] interface.
func (s Store) Close(_ context.Context) error {
	merr := s.DB.Merge(false)
	if errors.Is(merr, rosedb.ErrDBClosed) {
		merr = nil
	}
	cerr := s.DB.Close()
	return errors.Join(merr, cerr)
}

// KV implements the [blob.KV] interface using a rosedb database.
type KV struct {
	db     *rosedb.DB
	prefix dbkey.Prefix
}

// Open creates a [KV] by opening the rosedb database at path.
func Open(path string, opts *Options) (Store, error) {
	db, err := rosedb.Open(rosedb.Options{
		DirPath:           path,
		SegmentSize:       1 << 30,
		AutoMergeCronExpr: "* * * * *", // once a minute
	})
	if err != nil {
		return Store{}, err
	}
	return Store{M: monitor.New(db, "", func(c monitor.Config[*rosedb.DB]) KV {
		return KV{db: c.DB, prefix: c.Prefix}
	})}, nil
}

// Get implements part of the [blob.KV] interface.
func (s KV) Get(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, blob.ErrKeyNotFound
	}
	data, err := s.db.Get([]byte(s.prefix.Add(key)))
	if errors.Is(err, rosedb.ErrKeyNotFound) {
		return nil, blob.KeyNotFound(key)
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

// Put implements part of the [blob.KV] interface.
func (s KV) Put(ctx context.Context, opts blob.PutOptions) error {
	if opts.Key == "" {
		return blob.ErrKeyNotFound
	}
	bkey := []byte(s.prefix.Add(opts.Key))
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

// Delete implements part of the [blob.KV] interface.
func (s KV) Delete(ctx context.Context, key string) error {
	if key == "" {
		return blob.ErrKeyNotFound
	}
	bkey := []byte(s.prefix.Add(key))
	ok, err := s.db.Exist(bkey)
	if err != nil {
		return err
	} else if !ok {
		return blob.KeyNotFound(key)
	}
	return s.db.Delete(bkey)
}

// List implements part of the [blob.KV] interface.
func (s KV) List(ctx context.Context, start string, f func(string) error) error {
	var ferr error
	bstart := []byte(s.prefix.Add(start))
	s.db.AscendGreaterOrEqual(bstart, func(key, _ []byte) (bool, error) {
		if !bytes.HasPrefix(key, []byte(s.prefix)) {
			return false, nil // no longer in our range
		}
		dkey := s.prefix.Remove(string(key))
		if err := f(dkey); errors.Is(err, blob.ErrStopListing) {
			return false, nil
		} else if err != nil {
			ferr = err
			return false, nil
		}
		return true, nil
	})
	return ferr
}

// Len implements part of the [blob.KV] interface.
func (s KV) Len(ctx context.Context) (int64, error) {
	var n int64
	err := s.List(ctx, "", func(string) error {
		n++
		return nil
	})
	return n, err
}

// Options provide options for opening a rosedb database.
// A nil *Options is ready for use and provides default values.
type Options struct{}
