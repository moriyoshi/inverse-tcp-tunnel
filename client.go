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

package inverse_tcp_tunnel

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/net/proxy"
)

type ClientConfig struct {
	Target1  string
	Target2  string
	ProxyURL string
	Logger   zerolog.Logger
}

type upstreamConn struct {
	mu         sync.Mutex
	dsAddr     addrKey
	conn       net.Conn
	closed     bool
	writerChan chan []byte
}

type clientPacket struct {
	upstream *upstreamConn
	payload  []byte
}

type Client struct {
	logger                zerolog.Logger
	reconnectionInterval  time.Duration
	downstreamConnMu      sync.Mutex
	downstreamConn        net.Conn
	downstreamDialer      func(context.Context) (net.Conn, error)
	upstreamDialer        func(context.Context) (net.Conn, error)
	upstreamConnsMu       sync.Mutex
	upstreamConns         map[net.Conn]*upstreamConn
	upstreamConnsByDSAddr map[addrKey]*upstreamConn
	packetChan            chan clientPacket

	connRequestChan     chan addrKey
	connCloseNoticeChan chan addrKey

	gracefulShutdownChan chan struct{}
}

func doDial(ctx context.Context, dialer proxy.ContextDialer, addrRepr string) (net.Conn, error) {
	var af, addr string
	if strings.HasPrefix(addrRepr, "unix:") {
		af = "unix"
		addr = addrRepr[5:]
	} else if strings.HasPrefix(addrRepr, "tcp:") {
		af = "tcp"
		addr = addrRepr[4:]
	} else {
		af = "tcp"
		addr = addrRepr
	}
	return dialer.DialContext(ctx, af, addr)
}

func (c *Client) handleUpstream(ctx context.Context, wg *sync.WaitGroup, addr addrKey) {
	ctx, cancel := context.WithCancel(ctx)
	lc := c.logger.With().Str("addr", addr.String()).Logger()
	l := lc.With().Str("module", "upstream_handler").Logger()
	defer l.Debug().Msg("terminating")
	defer cancel()
	defer wg.Done()
	l.Debug().Msg("launched")

	l.Info().Msg("connecting to the upstream")
	var err error
	upstream := &upstreamConn{dsAddr: addr, writerChan: make(chan []byte)}
	upstream.conn, err = c.upstreamDialer(ctx)
	if err != nil {
		l.Error().Msg(fmt.Sprintf("failed to connect to the upstream: %s", err.Error()))
		err = sendCommand(c.downstreamConn, translateToErrorCode(err), &addr)
		if err != nil {
			l.Error().Err(err)
		}
		return
	}
	l.Info().Msg("connected to the upstream")

	lc = lc.With().Str("remote_addr", upstream.conn.RemoteAddr().String()).Str("local_addr", upstream.conn.LocalAddr().String()).Logger()

	c.upstreamConnsMu.Lock()
	c.upstreamConns[upstream.conn] = upstream
	c.upstreamConnsByDSAddr[upstream.dsAddr] = upstream
	c.upstreamConnsMu.Unlock()

	defer func() {
		c.upstreamConnsMu.Lock()
		delete(c.upstreamConns, upstream.conn)
		delete(c.upstreamConnsByDSAddr, upstream.dsAddr)
		c.upstreamConnsMu.Unlock()
	}()

	var innerWg sync.WaitGroup
	innerWg.Add(1)
	// reader
	go func(wg *sync.WaitGroup) {
		l := lc.With().Str("module", "upstream_handler/reader").Logger()
		defer l.Debug().Msg("terminating")
		defer cancel()
		defer wg.Done()
		l.Debug().Msg("launched")
		buf := make([]byte, 131072)
	outer:
		for {
			n, err := upstream.conn.Read(buf)
			if err != nil {
				if !os.IsTimeout(err) && err != io.EOF {
					l.Error().Msg(fmt.Sprintf("failed to read: %s", err.Error()))
				}
				break outer
			}
			payload := make([]byte, n)
			copy(payload, buf[:n])
			c.packetChan <- clientPacket{upstream, payload}
		}
	}(&innerWg)

	innerWg.Add(1)
	// writer
	go func(wg *sync.WaitGroup) {
		l := lc.With().Str("module", "upstream_handler/writer").Logger()
		defer l.Debug().Msg("terminating")
		defer upstream.conn.Close()
		defer cancel()
		defer wg.Done()
		l.Debug().Msg("launched")
	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case payload, ok := <-upstream.writerChan:
				if !ok {
					break outer
				}
				n, err := upstream.conn.Write(payload)
				if err != nil {
					if !os.IsTimeout(err) && err != io.EOF {
						l.Error().Msg(fmt.Sprintf("failed to write: %s", err.Error()))
					}
					break outer
				}
				if n != len(payload) {
					l.Error().Msg(fmt.Sprintf("failed to write %d bytes (%d bytes actual)", len(payload), n))
				}
			}
		}
	}(&innerWg)

	l.Info().Msg("sending reply for a connection request")
	err = sendCommand(c.downstreamConn, cmdSuccess, &addr)
	if err != nil {
		l.Error().Err(err)
	}
	innerWg.Wait()

	l.Info().Msg("sending close notice")
	err = sendCommand(c.downstreamConn, cmdConnClose, &addr)
	if err != nil {
		l.Error().Err(err)
	}
}

func (c *Client) handleDownstreamConn(ctx context.Context, wg *sync.WaitGroup) {
	lc := c.logger.With().Str("remote_addr", c.downstreamConn.RemoteAddr().String()).Logger()
	l := lc.With().Str("module", "downstrem_handler").Logger()
	ctx, cancel := context.WithCancel(ctx)
	defer l.Debug().Msg("terminating")
	defer func() {
		l.Info().Msg(fmt.Sprintf("downstream connection (%s) closed", c.downstreamConn.RemoteAddr().String()))
		c.downstreamConnMu.Lock()
		defer c.downstreamConnMu.Unlock()
		c.downstreamConn.Close()
		c.downstreamConn = nil
	}()
	defer cancel()
	defer wg.Done()
	l.Debug().Msg("launched")

	var innerWg sync.WaitGroup

	innerWg.Add(1)
	// reader
	go func(wg *sync.WaitGroup) {
		l := lc.With().Str("module", "dowwnstream_handler/reader").Logger()
		defer l.Debug().Msg("terminating")
		defer cancel()
		defer wg.Done()
		l.Debug().Msg("launched")
	outer:
		for {
			var cmd int
			var addr addrKey
			var payload []byte
			err := recvRequestOrPacket(c.downstreamConn, &cmd, &addr, &payload)
			if err != nil {
				if !os.IsTimeout(err) {
					l.Error().Err(err)
				}
				break outer
			}
			if payload == nil {
				switch cmd {
				case cmdConnect:
					l.Info().Func(func(e *zerolog.Event) { e.Msg(fmt.Sprintf("received connection request: %s", addr.String())) })
					c.connRequestChan <- addr
				case cmdCloseNotify:
					l.Info().Func(func(e *zerolog.Event) {
						e.Msg(fmt.Sprintf("received connection close notification: %s", addr.String()))
					})
					c.connCloseNoticeChan <- addr
				}
			} else {
				l.Debug().Func(func(e *zerolog.Event) { e.Msg(fmt.Sprintf("received packet of %d bytes", len(payload))) })
				c.upstreamConnsMu.Lock()
				upstream, ok := c.upstreamConnsByDSAddr[addr]
				c.upstreamConnsMu.Unlock()
				if !ok {
					l.Error().Msg("upstream has gone")
					continue
				}
				upstream.mu.Lock()
				closed := upstream.closed
				writerChan := upstream.writerChan
				upstream.mu.Unlock()
				if !closed {
					writerChan <- payload
				}
			}
		}
	}(&innerWg)

	innerWg.Add(1)
	// writer
	go func(wg *sync.WaitGroup) {
		l := lc.With().Str("module", "dowwnstream_handler/writer").Logger()
		defer l.Debug().Msg("terminating")
		defer cancel()
		defer c.downstreamConn.Close()
		defer wg.Done()
		l.Debug().Msg("launched")
	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case p, ok := <-c.packetChan:
				if !ok {
					break outer
				}
				l.Debug().Func(func(e *zerolog.Event) { e.Msg(fmt.Sprintf("sending packet of %d bytes", len(p.payload))) })
				sendPacket(c.downstreamConn, cmdPacket, p.upstream.dsAddr.ToTCPAddr(), p.payload)
			}
		}
	}(&innerWg)

	innerWg.Wait()
}

func (c *Client) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	l := c.logger

	var wg sync.WaitGroup

	t := time.NewTicker(c.reconnectionInterval)
	defer t.Stop()

	wg.Add(1)
	// event handler
	go func(wg *sync.WaitGroup) {
		l := l.With().Str("module", "event_handler").Logger()
		defer l.Debug().Msg("terminating")
		defer cancel()
		defer wg.Done()
		l.Debug().Msg("launched")
	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case r, ok := <-c.connRequestChan:
				if !ok {
					break outer
				}
				wg.Add(1)
				go c.handleUpstream(ctx, wg, r)
			case r, ok := <-c.connCloseNoticeChan:
				if !ok {
					break outer
				}
				c.upstreamConnsMu.Lock()
				upstream, ok := c.upstreamConnsByDSAddr[r]
				c.upstreamConnsMu.Unlock()
				// upstream is not gone yet
				if ok {
					upstream.mu.Lock()
					if !upstream.closed {
						upstream.closed = true
						close(upstream.writerChan)
					}
					upstream.mu.Unlock()
				}
			}
		}
	}(&wg)

	wg.Add(1)
	// downstream watchdog
	go func(wg *sync.WaitGroup) {
		l := l.With().Str("module", "downstream_watchdog").Logger()
		defer l.Debug().Msg("terminating")
		defer cancel()
		defer wg.Done()
		l.Debug().Msg("launched")
		for {
			select {
			case <-t.C:
				go func() {
					c.downstreamConnMu.Lock()
					defer c.downstreamConnMu.Unlock()
					if c.downstreamConn == nil {
						l.Info().Msg("downstream is unconnected. trying to connect...")
						downstreamConn, err := c.downstreamDialer(ctx)
						if err != nil {
							l.Error().Msg(fmt.Sprintf("failed to connect to downstream: %s", err.Error()))
							return
						}
						l.Info().Msg(
							fmt.Sprintf(
								"downstream connection establised: %s => %s",
								downstreamConn.LocalAddr().String(),
								downstreamConn.RemoteAddr().String(),
							),
						)
						c.downstreamConn = downstreamConn
						wg.Add(1)
						go c.handleDownstreamConn(ctx, wg)
					}
				}()
			}
		}
	}(&wg)
	wg.Wait()
	return nil
}

func chooseDialer(defaultDialer, proxyDialer proxy.ContextDialer, target string) func(context.Context) (net.Conn, error) {
	if strings.HasPrefix(target, "proxy:") {
		target := target[6:]
		return func(ctx context.Context) (net.Conn, error) {
			if proxyDialer == nil {
				return nil, fmt.Errorf("no proxy connection is configured for %s", target)
			}
			return doDial(ctx, proxyDialer, target)
		}
	} else {
		return func(ctx context.Context) (net.Conn, error) {
			return doDial(ctx, defaultDialer, target)
		}
	}
}

func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	var downstreamConn net.Conn
	var err error
	defer func() {
		if err != nil {
			if downstreamConn != nil {
				downstreamConn.Close()
			}
		}
	}()
	defaultDialer := &net.Dialer{}
	var proxyDialer proxy.ContextDialer
	if config.ProxyURL != "" {
		proxyURL, err := url.Parse(config.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy url: %s: %w", config.ProxyURL, err)
		}
		_proxyDialer, err := proxy.FromURL(proxyURL, defaultDialer)
		if err != nil {
			return nil, fmt.Errorf("failed to create a proxy dialer: %w", err)
		}
		var ok bool
		proxyDialer, ok = _proxyDialer.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("proxy dialer does not implement ContextDialer")
		}
	}

	return &Client{
		reconnectionInterval:  time.Second,
		downstreamDialer:      chooseDialer(defaultDialer, proxyDialer, config.Target1),
		upstreamDialer:        chooseDialer(defaultDialer, proxyDialer, config.Target2),
		upstreamConns:         make(map[net.Conn]*upstreamConn),
		upstreamConnsByDSAddr: make(map[addrKey]*upstreamConn),
		packetChan:            make(chan clientPacket, 1),
		connRequestChan:       make(chan addrKey, 1),
		connCloseNoticeChan:   make(chan addrKey, 1),
		logger:                config.Logger,
		gracefulShutdownChan:  make(chan struct{}),
	}, nil
}
