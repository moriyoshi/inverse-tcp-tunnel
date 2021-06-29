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
	var config inverse_tcp_tunnel.ServerConfig
	var logLevelStr string
	flag.StringVar(&config.Bind1, "listen", "", "address to listen (acceptor)")
	flag.StringVar(&config.Bind2, "tunnel", "", "address to listen (tunnel)")
	flag.StringVar(&logLevelStr, "loglevel", "info", "loglevel")
	flag.Parse()

	logLevel, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		message(fmt.Sprintf("unknown level string '%s'", logLevelStr))
		os.Exit(255)
	}

	logger = logger.Level(logLevel)

	config.Logger = logger
	server, err := inverse_tcp_tunnel.NewServer(config)
	if err == nil {
		err = server.Run(context.Background())
	}
	if err != nil {
		message(err.Error())
		os.Exit(1)
	}
}
