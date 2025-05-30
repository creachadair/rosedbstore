// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

// Package rosedbstore implements the [blob.StoreCloser] interface on [rosedb].
//
// [rosedb]: https://github.com/rosedblabs/rosedb
package rosedbstore

import (
	"bytes"
	"context"
	"errors"
	"iter"

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

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(_ context.Context) error {
	merr := s.DB.Merge(true)
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
	return Store{M: monitor.New(monitor.Config[*rosedb.DB, KV]{
		DB: db,
		NewKV: func(_ context.Context, db *rosedb.DB, pfx dbkey.Prefix, _ string) (KV, error) {
			return KV{db: db, prefix: pfx}, nil
		},
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

// Has implements part of the [blob.KV] interface.
func (s KV) Has(ctx context.Context, keys ...string) (blob.KeySet, error) {
	var out blob.KeySet
	for _, key := range keys {
		_, err := s.db.Get([]byte(s.prefix.Add(key)))
		if errors.Is(err, rosedb.ErrKeyNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		out.Add(key)
	}
	return out, nil
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
func (s KV) List(ctx context.Context, start string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		bstart := []byte(s.prefix.Add(start))
		s.db.AscendGreaterOrEqual(bstart, func(key, _ []byte) (bool, error) {
			if !bytes.HasPrefix(key, []byte(s.prefix)) {
				return false, nil // no longer in our range
			}
			dkey := s.prefix.Remove(string(key))
			if !yield(dkey, nil) {
				return false, nil
			}
			return true, nil
		})
	}
}

// Len implements part of the [blob.KV] interface.
func (s KV) Len(ctx context.Context) (int64, error) {
	var n int64
	for _, err := range s.List(ctx, "") {
		if err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}

// Options provide options for opening a rosedb database.
// A nil *Options is ready for use and provides default values.
type Options struct{}
