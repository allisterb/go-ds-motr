package motrds

import (
	"context"
	"hash/fnv"

	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"

	mio "github.com/allisterb/go-ds-motr/mio"
)

//var _ ds.Datastore = (*Motr) (nil)

type MotrDatastore struct {
	Config
	mio.Mkv
}

type Config struct {
	LocalAddr       string
	HaxAddr         string
	ProfileFid      string
	LocalProcessFid string
	Idx             string
	Threads         int
	EnableTrace     bool
}

var log = logging.Logger("motrds")
var hash128 = fnv.New128()
var mkv mio.Mkv

func NewMotrDatastore(conf Config) (*MotrDatastore, error) {
	if rinit, einit := mio.Init(&conf.LocalAddr, &conf.HaxAddr, &conf.ProfileFid, &conf.LocalProcessFid, conf.Threads, conf.EnableTrace); !rinit {
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
		return &MotrDatastore{conf, mkv}, nil
	}
}

func (d *MotrDatastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	return mkv.Has(getOID(key))
}

func (d *MotrDatastore) Get(ctx context.Context, key ds.Key) (value []byte, err error) {
	return mkv.Get(getOID(key))
}

func (d *MotrDatastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	return mkv.GetSize(getOID(key))
}

func Close() error {
	return mkv.Close()
}

func getOID(key ds.Key) []byte {
	return hash128.Sum(key.Bytes())
}
