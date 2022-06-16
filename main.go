/*
 * Based on: https://github.com/Seagate/cortx-motr/blob/main/bindings/go/mkv/mkv.go
 * mkv.go has the following copyright notice:
 *
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

	logging "github.com/ipfs/go-log/v2"
	"github.com/mbndr/figlet4go"

	"github.com/allisterb/go-ds-motr/mio"
)

var log = logging.Logger("Go-Ds-Motr CLI")

func usage() {
	log.Errorf("Usage: %s [options] index_id key [value].\nWith [value] present it will be PUT operation, without value it will be GET operation.\nPrinting usage...", os.Args[0])
	flag.PrintDefaults()
}

var localEP string
var haxEP string
var profile string
var procFid string
var createFlag bool
var updateFlag bool
var deleteFlag bool
var traceOn bool
var threadsN int

func checkArg(arg *string, name string) {
	if *arg == "" {
		log.Errorf("%s: %s must be specified\n\n", os.Args[0], name)
		flag.Usage()
		os.Exit(1)
	}
}

func init() {
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

	flag.Parse()
	if flag.NArg() != 2 && flag.NArg() != 3 {
		usage()
		os.Exit(1)
	}
	checkArg(&localEP, "local endpoint (-ep)")
	checkArg(&haxEP, "hax endpoint (-hax)")
	checkArg(&profile, "cluster profile fid (-prof)")
	checkArg(&procFid, "local process fid (-proc)")

	indexID := flag.Arg(0)

	mio.Init(&localEP, &haxEP, &profile, &procFid, threadsN, traceOn)

	var mkv mio.Mkv
	if err := mkv.Open(indexID, createFlag); err != nil {
		log.Fatalf("failed to open index %v: %v", indexID, err)
	}
	defer mkv.Close()

	if flag.NArg() == 3 {
		if deleteFlag {
			log.Errorf("cannot delete and put at the same time")
			usage()
			os.Exit(1)
		}
		err := mkv.Put([]byte(flag.Arg(1)), []byte(flag.Arg(2)), updateFlag)
		if err != nil {
			log.Fatalf("failed to put: %v", err)
		}
	} else {
		if deleteFlag {
			err := mkv.Delete([]byte(flag.Arg(1)))
			if err != nil {
				log.Fatalf("failed to delete: %v", err)
			}
		} else {
			value, err := mkv.Get([]byte(flag.Arg(1)))
			if err != nil {
				log.Fatalf("failed to get: %v", err)
			}
			fmt.Printf("%s\n", value)
		}
	}
}

// vi: sw=4 ts=4 expandtab ai
