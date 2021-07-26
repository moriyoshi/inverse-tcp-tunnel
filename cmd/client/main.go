// Copyright (c) 2021 Moriyoshi Koizumi.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/moriyoshi/inverse-tcp-tunnel"
	"github.com/rs/zerolog"
)

var progName = filepath.Base(os.Args[0])

var logger = zerolog.New(os.Stderr)

func message(v string) {
	fmt.Fprintf(os.Stderr, "%s: %s\n", progName, v)
}

func main() {
	var config inverse_tcp_tunnel.ClientConfig
	var logLevelStr string
	flag.StringVar(&config.Target1, "downstream", "", "address to connect (downstream)")
	flag.StringVar(&config.Target2, "upstream", "", "address to connect (upstream)")
	flag.StringVar(&logLevelStr, "loglevel", "info", "loglevel")
	flag.StringVar(&config.ProxyURL, "proxy", "", "proxy URL (e.g. socks5://localhost:1080)")
	flag.Parse()

	logLevel, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		message(fmt.Sprintf("unknown level string '%s'", logLevelStr))
		os.Exit(255)
	}
	logger = logger.Level(logLevel)
	config.Logger = logger

	ctx := context.Background()
	client, err := inverse_tcp_tunnel.NewClient(ctx, config)
	if err == nil {
		err = client.Run(ctx)
	}
	if err != nil {
		message(err.Error())
		os.Exit(1)
	}
}
