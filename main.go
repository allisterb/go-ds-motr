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
	"os"
	"strconv"
	"strings"

	"github.com/alecthomas/kong"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mbndr/figlet4go"

	"github.com/allisterb/go-ds-motr/uint128"
)

type OidCmd struct {
	Name  string `arg:"" name:"name" help:"Object id or key name to generate 128-bit object id for."`
	Parse bool   `help:"Parse name as 128-bit object id." short:"P"`
}

// Command-line arguments
var CLI struct {
	Debug bool   `help:"Enable debug mode."`
	Oid   OidCmd `cmd:"" help:"Generate or parse Motr object id."`
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
		// Colors can be given by default ansi color codes...
		figlet4go.ColorGreen,
		figlet4go.ColorYellow,
		figlet4go.ColorCyan,
	}
	// The underscore would be an error
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
		ParseOID(l.Name)

	}
	return nil
}

func ParseOID(id string) {
	var oid uint128.Uint128
	var _err error
	if strings.Contains(id, ":") {
		var _lo, _hi uint64
		a := strings.Split(id, ":")
		hi := a[0]
		lo := a[1]
		if strings.Contains(lo, "0x") {
			lo = strings.ReplaceAll(lo, "0x", "")
		}
		if _lo, _err = strconv.ParseUint(lo, 10, 64); _err != nil {
			log.Fatalf("Could not parse low id %s as uint64: %w.", lo, _err)
		}
		if strings.Contains(hi, "0x") {
			hi = strings.ReplaceAll(hi, "0x", "")
		}
		if _hi, _err = strconv.ParseUint(hi, 10, 64); _err != nil {
			log.Fatalf("Could not parse hi id %s as uint64: %w.", hi, _err)
		}
		oid = uint128.FromInts(_hi, _lo)
	} else {
		name := id
		if strings.Contains(name, "0x") {
			name = strings.ReplaceAll(name, "0x", "")
		}
		if oid, _err = uint128.FromString(name); _err != nil {
			log.Fatalf("Could not parse name %s as uint128: %s.", name, _err)
		}
	}
	log.Infof("128-bit OID is 0x%x:0x%x\n", oid.Hi, oid.Lo)
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
