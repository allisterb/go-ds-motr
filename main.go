package main

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"github.com/alecthomas/kong"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mbndr/figlet4go"

	"github.com/allisterb/go-ds-motr/mio"
	"github.com/allisterb/go-ds-motr/uint128"
)

type OidCmd struct {
	Name  string `arg:"" name:"name" help:"Object id or key name to generate 128-bit object id for."`
	Parse bool   `help:"Parse name as 128-bit object id." short:"P"`
}

type IdxCmd struct {
	Name   string `arg:"" name:"name" help:"Name of index to create."`
	Delete bool   `help:"Parse name as 128-bit object id." short:"P"`
}

type StoreCmd struct {
	LocalEP    string `required:"" name:"local" short:"L" help:"Motr local endpoint address."`
	HaxEP      string `required:"" name:"hax" short:"H" help:"Motr local endpoint address."`
	ProfileFid string `required:"" name:"profile" short:"C" help:"Cluster profile fid."`
	ProcessFid string `required:"" name:"process" short:"P" help:"Local process fid."`
	Idx        string `arg:"" name:"index" required:"" help:"Index ID."`
	Key        string `arg:"" name:"key" required:"" help:"Key name."`
	Value      string `arg:"" default:"" name:"key" help:"Value to store, or omit to retrieve the value stored at this key."`
	Delete     bool   `help:"Delete object identified by this key." short:"d"`
	Update     bool   `help:"Update object identified by this key with a new value." short:"u"`
	File       bool   `help:"Update object identified by this key with a new value." short:"f"`
}

var log = logging.Logger("CLI")
var hash128 = fnv.New128()

// Command-line arguments
var CLI struct {
	Debug bool     `help:"Enable debug mode."`
	Oid   OidCmd   `cmd:"" help:"Generate or parse Motr object id."`
	Store StoreCmd `cmd:"" help:"Store an object in the Motr key-value store."`
}

func init() {
	logging.SetAllLoggers(logging.LevelInfo)
}

func main() {
	ascii := figlet4go.NewAsciiRender()
	options := figlet4go.NewRenderOptions()
	options.FontColor = []figlet4go.Color{
		figlet4go.ColorGreen,
		figlet4go.ColorYellow,
		figlet4go.ColorCyan,
	}
	renderStr, _ := ascii.RenderOpts("Go-Ds-Motr", options)
	fmt.Print(renderStr)
	ctx := kong.Parse(&CLI)
	if contains(ctx.Args, "--debug") {
		logging.SetAllLoggers(logging.LevelInfo)
		log.Info("Debug mode enabled.")
	}
	ctx.FatalIfErrorf(ctx.Run(&kong.Context{}))
}

func (l *OidCmd) Run(ctx *kong.Context) error {
	if l.Parse {
		parseOID(l.Name)
	} else {
		createOID(l.Name)
	}
	return nil
}

func (s *StoreCmd) Run(ctx *kong.Context) error {
	if rinit, einit := mio.Init(&s.LocalEP, &s.HaxEP, &s.ProfileFid, &s.ProcessFid, 1, false); !rinit {
		log.Fatalf("Error initializing Motr client: %s", einit)
	} else {
		log.Info(("Initialized Motr client."))
	}
	if s.Value == "" {
		if s.Delete {
			deleteObject(s.Idx, s.Key)
		} else {
			selectObject(s.Idx, s.Key)
		}
	} else {
		createObject(s.Idx, s.Key, []byte(s.Value), s.Update)
	}
	return nil
}

func parseOID(id string) {
	var _lo, _hi uint64
	var oid uint128.Uint128
	var _err error
	if !strings.Contains(id, ":") {
		log.Fatalf("128-bit OID must be specified as <hi addr>:<lo addr>")
	}
	a := strings.Split(id, ":")
	hi := a[0]
	lo := a[1]
	if strings.Contains(hi, "0x") {
		log.Infof("Parsing hi addr as hexadecimal...")
		if _hi, _err = strconv.ParseUint(strings.ReplaceAll(hi, "0x", ""), 16, 64); _err != nil {
			log.Fatalf("Could not parse hi id %s as uint64: %s.", hi, _err)
		}
	} else {
		log.Infof("Parsing hi addr as decimal...")
		if _hi, _err = strconv.ParseUint(hi, 10, 64); _err != nil {
			log.Fatalf("Could not parse hi id %s as uint64: %s.", hi, _err)
		}
	}
	if strings.Contains(lo, "0x") {
		log.Infof("Parsing lo addr as hexadecimal...")
		if _lo, _err = strconv.ParseUint(strings.ReplaceAll(lo, "0x", ""), 16, 64); _err != nil {
			log.Fatalf("Could not parse lo id %s as uint64: %s.", lo, _err)
		}
	} else {
		log.Infof("Parsing lo addr as decimal...")
		if _lo, _err = strconv.ParseUint(lo, 10, 64); _err != nil {
			log.Fatalf("Could not parse lo id %s as uint64: %s.", lo, _err)
		}
	}
	oid = uint128.FromInts(_hi, _lo)
	log.Infof("128-bit OID is 0x%x:0x%x\n", oid.Hi, oid.Lo)
}

func createOID(name string) {
	log.Infof("Creating 128-bit OID for key name %s usng FNV-1 hash...")

	oid := uint128.FromBytes(hash128.Sum([]byte(name)))
	log.Infof("128-bit OID is 0x%x:0x%x\n", oid.Hi, oid.Lo)
}

func createObject(idx string, key string, data []byte, update bool) {
	var mkv mio.Mkv
	if err := mkv.Open(idx, false); err != nil {
		log.Fatalf("failed to open index %v: %v", idx, err)
	} else {
		log.Infof("Initialized Motr key-value index %s.", idx)
	}
	defer mkv.Close()
	if pget := mkv.Put(hash128.Sum(hash128.Sum([]byte(key))), data, update); pget != nil {
		log.Errorf("Error putting object at key %s in index %s: %s.", key, idx, pget)
	} else {
		log.Infof("Put object at key %s in index %s", key, idx)
	}
	mkv.Close()
}

func deleteObject(idx string, key string) {
	var mkv mio.Mkv
	if err := mkv.Open(idx, false); err != nil {
		log.Fatalf("failed to open index %v: %v", idx, err)
	} else {
		log.Infof("initialized Motr key-value index %s.", idx)
	}
	defer mkv.Close()
	oid := hash128.Sum(hash128.Sum([]byte(key)))
	if edel := mkv.Delete(oid); edel != nil {
		log.Errorf("Error deleting key %s: %s.", key, edel)
	} else {
		log.Infof("Deleted key %s in index %s.", key, idx)
	}
}

func selectObject(idx string, key string) {
	var mkv mio.Mkv
	if err := mkv.Open(idx, false); err != nil {
		log.Fatalf("failed to open index %v: %v", idx, err)
	} else {
		log.Infof("initialized Motr key-value index %s.", idx)
	}
	defer mkv.Close()
	oid := hash128.Sum(hash128.Sum([]byte(key)))
	oid128 := uint128.FromBytes(oid)
	if rhas, ehas := mkv.Has(oid); rhas {
		if r, eget := mkv.Get(oid); eget != nil {
			log.Errorf("Error retrieving key %s: %s.", key, eget)
		} else {
			log.Infof("Key %s in index %s has oid: 0x%x:0x%x, value: %s.", key, idx, oid128.Hi, oid128.Lo, string(r))
		}
	} else {
		if ehas == nil {
			log.Infof("Key %s in index %s does not exist.", key, idx)
		} else {
			log.Fatalf("Error checking existence of key %s in index %s.", key, idx)
		}
	}
}
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// vi: sw=4 ts=4 expandtab ai
