package motrds

import (
	mio "github.com/allisterb/go-ds-motr/mio"
)

//var _ ds.Datastore = (*Motr) (nil)

type Motr struct {
	Config
	mio.Mkv
}

type Config struct {
	LocalAddr       string
	HaxAddr         string
	ProfileFid      string
	LocalProcessFid string
	Threads         int
	EnableTrace     bool
}

func NewMotrDatastore(conf Config) (*Motr, error) {
	if rinit, e := mio.InitLib(&conf.LocalAddr, &conf.HaxAddr, &conf.ProfileFid, &conf.LocalProcessFid, conf.Threads, conf.EnableTrace); !rinit {
		return nil, e
	}
	var mkv mio.Mkv
	return &Motr{conf, mkv}, nil

}
