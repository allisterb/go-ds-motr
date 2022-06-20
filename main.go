/*
 * Based on: https://github.com/Seagate/cortx-motr/blob/main/bindings/go/mkv/mkv.go
 * mkv.go has the following copyright notice:
 /

/*
 * Copyright (c) 2021 Seagate Technology LLC and/or its Affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For any questions about this software or licensing,
 * please email opensource@seagate.com or cortx-questions@seagate.com.
 *
 * Original author: Andriy Tkachuk <andriy.tkachuk@seagate.com>
 * Original creation date: 28-Apr-2021
*/

package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"
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

// Command-line arguments
var CLI struct {
	Debug bool     `help:"Enable debug mode."`
	Oid   OidCmd   `cmd:"" help:"Generate or parse Motr object id."`
	Store StoreCmd `cmd:"" help:"Store an object in the Motr key-value store."`
}

var log = logging.Logger("CLI")
var localEP string
var haxEP string
var profile string
var procFid string
var createFlag bool
var updateFlag bool
var deleteFlag bool
var traceOn bool
var threadsN int

func usage() {
	log.Errorf("Usage: go-ds-motr [options] index_id key [value].\nWith [value] present it will be PUT operation, without value it will be GET operation.\nPrinting usage...")
	flag.PrintDefaults()
}

func checkArg(arg *string, name string) {
	if *arg == "" {
		log.Errorf("%s: %s must be specified\n\n", os.Args[0], name)
		flag.Usage()
		os.Exit(1)
	}
}

func init() {
	logging.SetAllLoggers(logging.LevelInfo)
	flag.StringVar(&localEP, "ep", "", "local `endpoint` address")
	flag.StringVar(&haxEP, "hax", "", "hax `endpoint` address")
	flag.StringVar(&profile, "prof", "", "cluster profile `fid`")
	flag.StringVar(&procFid, "proc", "", "local process `fid`")
	flag.BoolVar(&createFlag, "c", false, "create index if not present")
	flag.BoolVar(&updateFlag, "u", false, "update value at the existing key")
	flag.BoolVar(&deleteFlag, "d", false, "delete the record by the key")

	// Optional
	flag.BoolVar(&traceOn, "trace", false, "generate m0trace.pid file")
	flag.IntVar(&threadsN, "threads", 1, "`number` of threads to use")

	flag.Usage = usage
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
		if s.Update {
			updateObject(s.Idx, s.Key, []byte(s.Value))
		} else {
			createObject(s.Idx, s.Key, []byte(s.Value))
		}
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
	h := fnv.New128()
	oid := uint128.FromBytes(h.Sum([]byte(name)))
	log.Infof("128-bit OID is 0x%x:0x%x\n", oid.Hi, oid.Lo)
}

func createObject(idx string, name string, data []byte) {

}

func updateObject(idx string, name string, data []byte) {

}

func deleteObject(idx string, name string) {

}

func selectObject(idx string, key string) {
	var mkv mio.Mkv
	log.Info("initialized Motr key-value store access.")
	if err := mkv.Open(idx, false); err != nil {
		log.Fatalf("failed to open index %v: %v", idx, err)
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
