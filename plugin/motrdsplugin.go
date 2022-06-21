package plugin

import (
	"fmt"

	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"

	"github.com/allisterb/go-ds-motr/motrds"
)

var Plugins = []plugin.Plugin{
	&MotrPlugin{},
}

type MotrPlugin struct{}

func (_ MotrPlugin) Name() string {
	return "motr-datastore-plugin"
}

func (_ MotrPlugin) Version() string {
	return "0.0.1"
}

func (_ MotrPlugin) Init(env *plugin.Environment) error {
	return nil
}

func (_ MotrPlugin) DatastoreTypeName() string {
	return "motrds"
}

func (mp MotrPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		localAddr, ok := m["localAddr"].(string)
		if !ok {
			return nil, fmt.Errorf("motrds: no local address specified")
		}

		haxAddr, ok := m["haxAddr"].(string)
		if !ok {
			return nil, fmt.Errorf("motords: no hax address specified")
		}

		profileFid, ok := m["profileFid"].(string)
		if !ok {
			return nil, fmt.Errorf("motrds: no cluster profile fid specified")
		}

		processFid, ok := m["processFid"].(string)
		if !ok {
			return nil, fmt.Errorf("motrds: no local process specified")
		}

		// Optional
		var threads int
		if v, ok := m["threads"]; ok {
			threadsf, ok := v.(float64)
			threads = int(threadsf)
			switch {
			case !ok:
				return nil, fmt.Errorf("motrds: threads not a number")
			case threads <= 0:
				return nil, fmt.Errorf("motrds: threads <= 0: %f", threadsf)
			case float64(threads) != threadsf:
				return nil, fmt.Errorf("motrds: threads is not an integer: %f", threadsf)
			}
		}
		var trace bool = false
		if v, ok := m["trace"]; ok {
			trace, ok = v.(bool)
			if !ok {
				return nil, fmt.Errorf("motrds: trace not a bool")
			}
		}
		return &MotrConfig{
			cfg: motrds.Config{
				LocalAddr:       localAddr,
				HaxAddr:         haxAddr,
				ProfileFid:      profileFid,
				LocalProcessFid: processFid,
			},
		}, nil
	}
}

type MotrConfig struct {
	cfg motrds.Config
}

func (mc *MotrConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{
		"localAddr":     mc.cfg.LocalAddr,
		"haxAddr":       mc.cfg.HaxAddr,
		"rootDirectory": s3c.cfg.RootDirectory,
	}
}

func (mc *MotrConfig) Create(path string) (repo.Datastore, error) {
	return motrds.NewMotrDatastore(mc.cfg)
}
