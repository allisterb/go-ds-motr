package motrds

import (
	"context"
	"hash/fnv"
	"sync"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/allisterb/go-ds-motr/mio"
)

var _ ds.Datastore = (*MotrDatastore)(nil)

type MotrDatastore struct {
	Config
	mio.Mkv
	Ldb  leveldb.DB
	Lock *sync.RWMutex
}

type Config struct {
	LocalAddr       string
	HaxAddr         string
	ProfileFid      string
	LocalProcessFid string
	Idx             string
	LevelDBPath     string
	Threads         int
	Trace           bool
}

var log = logging.Logger("motrds")
var hash128 = fnv.New128()
var mkv mio.Mkv

func NewMotrDatastore(conf Config) (*MotrDatastore, error) {
	if rinit, einit := mio.Init(&conf.LocalAddr, &conf.HaxAddr, &conf.ProfileFid, &conf.LocalProcessFid, conf.Threads, conf.Trace); !rinit {
		log.Errorf("Failed to initialize Motr client: %s.", einit)
		return nil, einit
	} else {
		log.Infof("Initialized Motr client.")
	}

	if eidx := mkv.Open(conf.Idx, false); eidx != nil {
		log.Errorf("Failed to open Motr key-value index %v: %v", conf.Idx, eidx)
		return nil, eidx
	} else {
		log.Infof("Initialized Motr key-value index %v.", conf.Idx)

	}
	ldbopt := new(opt.Options)
	var ldb leveldb.DB
	if _ldb, eldb := leveldb.OpenFile(conf.LevelDBPath, ldbopt); eldb != nil {
		log.Errorf("Failed to open LevelDB database at %s.", conf.LevelDBPath)
		return nil, eldb
	} else {
		ldb = *_ldb
		log.Infof("Opened LevelDB database at %v.", conf.LevelDBPath)
	}
	return &MotrDatastore{conf, mkv, ldb, new(sync.RWMutex)}, nil
}

func (d *MotrDatastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	log.Debugf("Check for existence of key %s..", string(key.Bytes()))
	return d.Ldb.Has(key.Bytes(), nil)
}

func (d *MotrDatastore) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	val, err := d.Ldb.Get(key.Bytes(), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return d.Mkv.Get(val)
}

func (d *MotrDatastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	return mkv.GetSize(getOID(key))
}

func (d *MotrDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	var rnge *util.Range
	// make a copy of the query for the fallback naive query implementation.
	// don't modify the original so res.Query() returns the correct results.
	qNaive := q
	prefix := ds.NewKey(q.Prefix).String()
	if prefix != "/" {
		rnge = util.BytesPrefix([]byte(prefix + "/"))
		qNaive.Prefix = ""
	}
	i := d.Ldb.NewIterator(rnge, nil)
	next := i.Next
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case query.OrderByKey, *query.OrderByKey:
			qNaive.Orders = nil
		case query.OrderByKeyDescending, *query.OrderByKeyDescending:
			next = func() bool {
				next = i.Prev
				return i.Last()
			}
			qNaive.Orders = nil
		default:
		}
	}
	r := query.ResultsFromIterator(q, query.Iterator{
		Next: func() (query.Result, bool) {
			d.Lock.RLock()
			defer d.Lock.RUnlock()
			if !next() {
				return query.Result{}, false
			}
			oid := hash128.Sum(i.Key())
			k := string(oid)
			var size int
			if _size, serr := mkv.GetSize(oid); serr != nil {
				return query.Result{}, true
			} else {
				size = _size
			}
			e := query.Entry{Key: k, Size: size}
			if !q.KeysOnly {
				if v, eval := mkv.Get(oid); eval != nil {
					e.Value = v
				}
			}
			return query.Result{Entry: e}, true
		},
		Close: func() error {
			d.Lock.RLock()
			defer d.Lock.RUnlock()
			i.Release()
			return nil
		},
	})
	return query.NaiveQueryApply(qNaive, r), nil
}

func (d *MotrDatastore) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	oid := getOID(key)
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	if emotr := mkv.Put(oid, value, false); emotr != nil {
		log.Errorf("Error putting key %v with OID %v to Motr index %s: %s", key, oid, d.Idx, emotr)
		return emotr
	}
	if eldb := d.Ldb.Put(key.Bytes(), []byte{1}, &opt.WriteOptions{Sync: true}); eldb != nil {
		log.Errorf("Error writing key %v to LevelDB: %s", key, eldb)
		mkv.Delete(getOID(key))
		return eldb
	} else {
		return nil
	}
}

func (d *MotrDatastore) Delete(ctx context.Context, key ds.Key) (err error) {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	if eldb := d.Ldb.Delete(key.Bytes(), &opt.WriteOptions{Sync: true}); eldb != nil {
		log.Errorf("Error deleting key %v from LevelDB: %s", key, eldb)
		return eldb
	}
	return mkv.Delete(getOID(key))
}

func (d *MotrDatastore) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

func (d *MotrDatastore) Close() error {
	d.Lock.Lock()
	defer d.Lock.Unlock()
	d.Ldb.Close()
	return mkv.Close()
}

func (d *MotrDatastore) Batch(ctx context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

func getOID(key ds.Key) []byte {
	return hash128.Sum(key.Bytes())
}