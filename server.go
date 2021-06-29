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
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type ServerConfig struct {
	Bind1  string
	Bind2  string
	Logger zerolog.Logger
}

type serverClientState int

const (
	notConnected = iota
	connEstablishing
	connEstablished
	connClosing
	connClosed
)

type serverClient struct {
	mu              sync.Mutex
	state           serverClientState
	key             addrKey
	conn            net.Conn
	writerChan      chan []byte
	connBoundTo     net.Conn
	egressSpoolHead *packet
	egressSpool     *packet
	awNext          *serverClient
	awPrev          *serverClient
	cloNext         *serverClient
	cloPrev         *serverClient
}

type packet struct {
	spNext          *packet
	freeNext        *packet
	client          *serverClient
	payload         []byte
	_embeddedBuffer [4096 - 8*3 - 8*3]byte
}

func (p *packet) send(dest net.Conn) error {
	addr, ok := p.client.conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("something goes really wrong")
	}
	return sendPacket(dest, cmdSend, *addr, p.payload)
}

func (p *packet) recv() error {
	n, err := p.client.conn.Write(p.payload)
	if err != nil {
		return fmt.Errorf("error occurred during sending an ingress packet: %w", err)
	}
	if len(p.payload) != n {
		return fmt.Errorf("failed to write %d bytes (%d bytes actual)", len(p.payload), n)
	}
	return nil
}

type Server struct {
	acceptorListener net.Listener
	tunnelListener   net.Listener

	logger zerolog.Logger

	clientMapMu        sync.Mutex
	acceptorClients    map[net.Conn]*serverClient
	addrToClientMap    map[addrKey]*serverClient
	tunnelClientConnMu sync.Mutex
	tunnelClientConn   net.Conn
	freePacketsHead    *packet
	freePackets        *packet
	freePacketsMu      sync.Mutex
	nAwClients         int
	awClientsHead      *serverClient
	awClients          *serverClient
	nCloClients        int
	cloClientsHead     *serverClient
	cloClients         *serverClient

	newClientChan   chan *serverClient
	clientCloseChan chan *serverClient
	connReadyChan   chan struct{}
	cliClosedChan   chan struct{}
	egressChan      chan *packet

	gracefulShutdownChan chan struct{}
}

var longLongAgo = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)

func (srv *Server) getFreePacket() *packet {
	var p *packet
	srv.freePacketsMu.Lock()
	if srv.freePackets == nil {
		p = new(packet)
	} else {
		p = srv.freePackets
		nextTail := p.freeNext
		p.freeNext = nil
		srv.freePackets = nextTail
		if nextTail == nil {
			srv.freePacketsHead = nil
		}
	}
	srv.freePacketsMu.Unlock()
	return p
}

func (srv *Server) putFreePacket(p *packet) {
	srv.freePacketsMu.Lock()
	if srv.freePacketsHead != nil {
		srv.freePacketsHead.freeNext = p
	} else {
		srv.freePackets = p
	}
	srv.freePacketsHead = p
	srv.freePacketsMu.Unlock()
}

func (srv *Server) handleAcceptorClient(ctx context.Context, c *serverClient, wg *sync.WaitGroup) {
	lc := srv.logger.With().Str("remote_addr", c.conn.RemoteAddr().String()).Logger()
	l := lc.With().Str("module", "acceptor_client_handler").Logger()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer l.Debug().Msg("terminating")
	defer func() {
		c.conn.Close()
		l.Info().Msg(fmt.Sprintf("client (%s) connection closed", c.key.String()))
		srv.clientCloseChan <- c
	}()
	defer wg.Done()
	l.Debug().Msg("launched")

	var innerWg sync.WaitGroup

	innerWg.Add(1)
	// reader
	go func(wg *sync.WaitGroup) {
		l := lc.With().Str("module", "acceptor_client_handler/reader").Logger()
		defer l.Debug().Msg("terminating")
		defer cancel()
		defer wg.Done()
		l.Debug().Msg("launched")
	outer:
		for {
			p := srv.getFreePacket()
			n, err := c.conn.Read(p._embeddedBuffer[:])
			if err != nil {
				if !os.IsTimeout(err) && err != io.EOF {
					l.Error().Msg(fmt.Sprintf("read error: %s", err.Error()))
				}
				srv.putFreePacket(p)
				break outer
			}
			l.Debug().Func(func(e *zerolog.Event) { e.Msg(fmt.Sprintf("received a packet of %d bytes", n)) })
			p.client = c
			p.payload = p._embeddedBuffer[:n]
			select {
			case <-ctx.Done():
				break outer
			case srv.egressChan <- p:
			}
		}
	}(&innerWg)

	innerWg.Add(1)
	// writer
	go func(wg *sync.WaitGroup) {
		l := lc.With().Str("module", "acceptor_client_handler/writer").Logger()
		defer l.Debug().Msg("terminating")
		defer c.conn.SetDeadline(longLongAgo)
		defer wg.Done()
		l.Debug().Msg("launched")
	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case payload, ok := <-c.writerChan:
				if !ok {
					break outer
				}
				l.Trace().Func(func(e *zerolog.Event) {
					b := make([]byte, hex.EncodedLen(len(payload)))
					hex.Encode(b, payload)
					e.Msg(fmt.Sprintf("%s => %s", c.key.String(), string(b)))
				})
				n, err := c.conn.Write(payload)
				if err != nil {
					if !os.IsTimeout(err) && err != io.EOF {
						l.Error().Msg(fmt.Sprintf("write failed: %s", err.Error()))
					}
					break outer
				}
				if n != len(payload) {
					l.Error().Msg(fmt.Sprintf("failed to write %d bytes (%d bytes actual)", len(payload), n))
					break outer
				}
			}
		}
	}(&innerWg)

	l.Info().Msg("establishing connection")
	innerWg.Wait()
}

func (srv *Server) handleTunnelClient(ctx context.Context, conn net.Conn, wg *sync.WaitGroup) {
	lc := srv.logger.With().Str("remote_addr", conn.RemoteAddr().String()).Logger()
	l := lc.With().Str("module", "tunnel_client_handler").Logger()
	ctx, cancel := context.WithCancel(ctx)
	defer l.Debug().Msg("terminating")
	defer cancel()
	defer wg.Done()
	l.Debug().Msg("launched")

	var innerWg sync.WaitGroup
	innerWg.Add(1)
	// tunnel event loop
	go func(wg *sync.WaitGroup) {
		l := lc.With().Str("module", "tunnel_client_handler/event_loop").Logger()
		defer l.Debug().Msg("terminating")
		defer cancel()
		defer srv.tunnelClientConn.Close()
		defer wg.Done()
		l.Debug().Msg("launched")
	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case _, ok := <-srv.connReadyChan:
				if !ok {
					break outer
				}
				srv.clientMapMu.Lock()
				awClientsHead := srv.awClientsHead
				awClients := srv.awClients
				nAwClients := srv.nAwClients
				srv.awClientsHead = nil
				srv.awClients = nil
				srv.nAwClients = 0
				srv.clientMapMu.Unlock()

				if !func() bool {
					l.Info().Msg(fmt.Sprintf("sending %d connection requests", nAwClients))
					var awNext *serverClient
					for p := awClients; p != nil; p = awNext {
						p.mu.Lock()
						awNext = p.awNext
						awPrev := p.awPrev
						if p.state != connEstablishing || p.connBoundTo == conn {
							p.mu.Unlock()
							continue
						}
						p.awPrev = nil
						p.awNext = nil
						if awPrev != nil {
							awPrev.awNext = awNext
						} else {
							awClients = awNext
						}
						if awNext != nil {
							awNext.awPrev = awPrev
						} else {
							awClientsHead = awPrev
						}
						nAwClients -= 1
						p.connBoundTo = conn
						p.mu.Unlock()
						l.Info().Msg(fmt.Sprintf("sending connection request for %s", p.key.String()))
						err := sendCommand(srv.tunnelClientConn, cmdConnect, &p.key)
						if err != nil {
							if !os.IsTimeout(err) && err != io.EOF {
								l.Error().Msg(fmt.Sprintf("error occurred within tunnel event loop: %s", err.Error()))
							}
							return false
						}
					}
					return true
				}() {
					// some error occurred; restore await list
					srv.clientMapMu.Lock()
					if srv.awClientsHead != nil {
						srv.awClientsHead.awNext = awClients
					} else {
						srv.awClients = awClients
					}
					awClients.awPrev = srv.awClientsHead
					srv.awClientsHead = awClientsHead
					srv.nAwClients += nAwClients
					srv.clientMapMu.Unlock()
				}
			case _, ok := <-srv.cliClosedChan:
				if !ok {
					break outer
				}
				srv.clientMapMu.Lock()
				cloClientsHead := srv.cloClientsHead
				cloClients := srv.cloClients
				nCloClients := srv.nCloClients
				srv.cloClientsHead = nil
				srv.cloClients = nil
				srv.nCloClients = 0
				srv.clientMapMu.Unlock()

				if !func() bool {
					l.Info().Msg(fmt.Sprintf("sending %d connection close notifications", nCloClients))
					var cloNext *serverClient
					for p := cloClients; p != nil; p = cloNext {
						p.mu.Lock()
						cloNext = p.cloNext
						cloPrev := p.cloPrev
						if p.state != connClosed || p.connBoundTo != conn {
							p.mu.Unlock()
							continue
						}
						p.cloPrev = nil
						p.cloNext = nil
						if cloPrev != nil {
							cloPrev.cloNext = cloNext
						} else {
							cloClients = cloNext
						}
						if cloNext != nil {
							cloNext.cloPrev = cloPrev
						} else {
							cloClientsHead = cloPrev
						}
						nCloClients -= 1
						p.connBoundTo = nil
						p.mu.Unlock()
						l.Info().Msg(fmt.Sprintf("sending connection close notification for %s", p.key.String()))
						err := sendCommand(srv.tunnelClientConn, cmdCloseNotify, &p.key)
						if err != nil {
							if !os.IsTimeout(err) && err != io.EOF {
								l.Error().Msg(fmt.Sprintf("error occurred within tunnel event loop: %s", err.Error()))
							}
							return false
						}
					}
					return true
				}() {
					// some error occurred; restore close list
					srv.clientMapMu.Lock()
					if srv.cloClientsHead != nil {
						srv.cloClientsHead.cloNext = cloClients
					} else {
						srv.cloClients = cloClients
					}
					cloClients.cloPrev = srv.cloClientsHead
					srv.cloClientsHead = cloClientsHead
					srv.nCloClients += nCloClients
					srv.clientMapMu.Unlock()
				}
			case p, ok := <-srv.egressChan:
				if !ok {
					break
				}
				p.client.mu.Lock()
				if p.client.state != connEstablished {
					l.Info().Msg("egress packet received, but the counterpart has not established the connection yet")
					if p.client.egressSpoolHead != nil {
						p.client.egressSpoolHead.spNext = p
					} else {
						p.client.egressSpool = p
					}
					p.client.egressSpoolHead = p
					p.client.mu.Unlock()
					break
				} else {
					p.client.mu.Unlock()
				}
				l.Trace().Func(func(e *zerolog.Event) {
					b := make([]byte, hex.EncodedLen(len(p.payload)))
					hex.Encode(b, p.payload)
					e.Msg(fmt.Sprintf("%s => %s", p.client.key.String(), string(b)))
				})
				err := p.send(srv.tunnelClientConn)
				srv.putFreePacket(p)
				if err != nil {
					if !os.IsTimeout(err) && err != io.EOF {
						l.Error().Msg(fmt.Sprintf("error occurred within tunnel event loop: %s", err.Error()))
					}
					break outer
				}
			}
		}
	}(&innerWg)

	innerWg.Add(1)
	// reader
	go func(wg *sync.WaitGroup) {
		l := lc.With().Str("module", "tunnel_client_handler/reader").Logger()
		defer l.Debug().Msg("terminating")
		defer cancel()
		defer func() {
			srv.tunnelClientConnMu.Lock()
			defer srv.tunnelClientConnMu.Unlock()
			srv.tunnelClientConn.Close()
			srv.tunnelClientConn = nil
		}()
		defer wg.Done()
		l.Debug().Msg("launched")
	outer:
		for {
			var addr addrKey
			var cmd int
			var payload []byte
			err := recvReplyOrPacket(srv.tunnelClientConn, &cmd, &addr, &payload)
			if err != nil {
				if !os.IsTimeout(err) && err != io.EOF {
					l.Error().Msg(fmt.Sprintf("failed to receive a packet: %s", err.Error()))
				}
				break outer
			}

			// resolve client from the client address
			srv.clientMapMu.Lock()
			c, ok := srv.addrToClientMap[addr]
			srv.clientMapMu.Unlock()
			if !ok {
				l.Error().Msg(fmt.Sprintf("unknown client: %s", addr.String()))
				continue
			}

			func() {
				c.mu.Lock()
				defer c.mu.Unlock()
				if payload == nil {
					if c.state == connEstablishing {
						srv.clientMapMu.Lock()
						srv.removeClientFromAwaitList(c)
						srv.clientMapMu.Unlock()
						if cmd == cmdSuccess {
							c.state = connEstablished
							var p *packet
							p = c.egressSpool
							c.egressSpool = nil
							c.egressSpoolHead = nil
							for ; p != nil; p = p.spNext {
								select {
								case <-ctx.Done():
									return
								case srv.egressChan <- p:
								}
							}
						} else {
							l.Error().Msg(fmt.Sprintf("upstream connection failed: %d", cmd))
							c.state = notConnected
							close(c.writerChan)
						}
						l.Info().Msg(fmt.Sprintf("connection established for %s", c.conn.RemoteAddr().String()))
					} else {
						if cmd == cmdConnClose {
							c.state = connClosing
							close(c.writerChan)
						} else {
							l.Error().Msg(fmt.Sprintf("no promise awaited for client: %p", c))
						}
					}
				} else {
					if cmd != cmdPacket {
						l.Error().Msg(fmt.Sprintf("invalid reply: %d", cmd))
						return
					}
					if c.state == connEstablished {
						select {
						case <-ctx.Done():
							return
						case c.writerChan <- payload:
						}
					}
				}
			}()
		}
	}(&innerWg)

	srv.clientMapMu.Lock()
	nAwClients := srv.nAwClients
	nCloClients := srv.nCloClients
	srv.clientMapMu.Unlock()
	if nAwClients > 0 {
		l.Debug().Func(func(e *zerolog.Event) {
			e.Msg(fmt.Sprintf("there are %d client(s) awaiting replies for connection requests", nAwClients))
		})
		srv.connReadyChan <- struct{}{}
		l.Debug().Msg("queued flush event for tunnel event handler")
	}
	if nCloClients > 0 {
		l.Debug().Func(func(e *zerolog.Event) {
			e.Msg(fmt.Sprintf("there are %d client(s) that have not had their close notifications sent", nCloClients))
		})
		srv.cliClosedChan <- struct{}{}
		l.Debug().Msg("queued flush event for tunnel event handler")
	}

	innerWg.Wait()
}

func (srv *Server) addClientToAwaitList(c *serverClient) {
	awClientsHead := srv.awClientsHead
	if awClientsHead == nil {
		srv.awClients = c
	} else {
		awClientsHead.awNext = c
		c.awPrev = awClientsHead
	}
	srv.awClientsHead = c
	srv.nAwClients += 1
}

func (srv *Server) removeClientFromAwaitList(c *serverClient) {
	if c.awPrev == nil && c.awNext == nil {
		return
	}
	if c == srv.awClients {
		srv.awClients = c.awNext
	} else {
		c.awPrev.awNext = c.awNext
	}
	if c == srv.awClientsHead {
		srv.awClientsHead = c.awPrev
	} else {
		c.awNext.awPrev = c.awPrev
	}
	c.awPrev = nil
	c.awNext = nil
	srv.nAwClients -= 1
}

func (srv *Server) addClientToClosingList(c *serverClient) {
	cloClientsHead := srv.cloClientsHead
	if cloClientsHead == nil {
		srv.cloClients = c
	} else {
		cloClientsHead.cloNext = c
		c.cloPrev = cloClientsHead
	}
	srv.cloClientsHead = c
	srv.nCloClients += 1
}

func (srv *Server) removeClientFromClosingList(c *serverClient) {
	if c.cloPrev == nil && c.cloNext == nil {
		return
	}
	if c == srv.cloClients {
		srv.cloClients = c.cloNext
	} else {
		c.cloPrev.cloNext = c.cloNext
	}
	if c == srv.cloClientsHead {
		srv.cloClientsHead = c.cloPrev
	} else {
		c.cloNext.cloPrev = c.cloPrev
	}
	c.cloPrev = nil
	c.cloNext = nil
	srv.nCloClients -= 1
}

func (srv *Server) launchAcceptors(ctx context.Context, wg *sync.WaitGroup) {
	lc := srv.logger.With().Str("local_addr", srv.acceptorListener.Addr().String()).Logger()
	wg.Add(1)
	// acceptor listener loop
	go func() {
		l := lc.With().Str("module", "acceptor_listener").Logger()
		defer l.Debug().Msg("terminating")
		defer srv.acceptorListener.Close()
		defer close(srv.newClientChan)
		defer close(srv.clientCloseChan)
		defer wg.Done()
		l.Debug().Msg("launched")
	outer:
		for {
			conn, err := srv.acceptorListener.Accept()
			if err != nil {
				if !os.IsTimeout(err) && err != io.EOF {
					l.Error().Msg(fmt.Sprintf("error occurred within acceptor listener loop: %s", err.Error()))
				}
				break
			}
			l.Info().Msg(fmt.Sprintf("accepted a request from %s", conn.RemoteAddr().String()))
			addr, ok := conn.RemoteAddr().(*net.TCPAddr)
			if !ok {
				l.Error().Msg("something goes really wrong")
				break
			}
			select {
			case <-ctx.Done():
				break outer
			case srv.newClientChan <- &serverClient{
				key:         addrKeyFromTCPAddr(addr),
				conn:        conn,
				connBoundTo: nil,
				state:       notConnected,
				writerChan:  make(chan []byte, 1),
			}:
			}
		}
	}()

	wg.Add(1)
	// acceptor event loop
	go func() {
		l := lc.With().Str("module", "acceptor_event_loop").Logger()
		defer l.Debug().Msg("terminating")
		defer wg.Done()
		l.Debug().Msg("launched")
		var innerWg sync.WaitGroup
	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case c, ok := <-srv.newClientChan:
				if !ok {
					break
				}
				c.state = connEstablishing
				c.awNext = nil
				srv.clientMapMu.Lock()
				srv.acceptorClients[c.conn] = c
				srv.addrToClientMap[c.key] = c
				srv.addClientToAwaitList(c)
				srv.clientMapMu.Unlock()
				innerWg.Add(1)
				go srv.handleAcceptorClient(ctx, c, &innerWg)
				srv.connReadyChan <- struct{}{}
			case c, ok := <-srv.clientCloseChan:
				if !ok {
					break
				}
				l.Debug().Func(func(e *zerolog.Event) { e.Msg(fmt.Sprintf("client (%s) deleted", c.key.String())) })
				srv.clientMapMu.Lock()
				c.mu.Lock()
				delete(srv.acceptorClients, c.conn)
				delete(srv.addrToClientMap, c.key)
				c.conn = nil
				c.state = connClosed
				srv.removeClientFromAwaitList(c)
				srv.addClientToClosingList(c)
				c.mu.Unlock()
				srv.clientMapMu.Unlock()
				srv.cliClosedChan <- struct{}{}
			}
		}
		innerWg.Wait()
	}()
}

func (srv *Server) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	// watchdog
	go func() {
		l := srv.logger.With().Str("module", "watchdog").Logger()
		defer l.Info().Msg("terminated")
		l.Info().Msg("launched")
		<-ctx.Done()
		l.Info().Msg("watchdog is closing listeners")
		srv.acceptorListener.Close()
		srv.tunnelListener.Close()
	}()

	srv.launchAcceptors(ctx, &wg)

	wg.Add(1)
	// tunnel listener loop
	go func() {
		l := srv.logger.With().Str("module", "tunnel_listener_loop").Str("local_addr", srv.tunnelListener.Addr().String()).Logger()
		defer l.Debug().Msg("terminating")
		defer wg.Done()
		defer srv.tunnelListener.Close()
		l.Debug().Msg("launched")
		for {
			conn, err := srv.tunnelListener.Accept()
			if err != nil {
				if !os.IsTimeout(err) && err != io.EOF {
					l.Error().Msg(fmt.Sprintf("error occurred within tunnel listener loop: %s", err.Error()))
				}
				break
			}
			defer l.Info().Msg(fmt.Sprintf("accepted a request from %s", conn.RemoteAddr().String()))
			func() {
				srv.tunnelClientConnMu.Lock()
				defer srv.tunnelClientConnMu.Unlock()
				if srv.tunnelClientConn != nil {
					l.Warn().Msg("there is already an established connection for the tunnel")
					conn.Close()
					return
				}
				srv.tunnelClientConn = conn
				wg.Add(1)
				go srv.handleTunnelClient(ctx, conn, &wg)
			}()

		}
	}()

	wg.Wait()
	return nil
}

func doListen(addrRepr string) (net.Listener, error) {
	var af, addr string
	if strings.HasPrefix(addrRepr, "unix:") {
		af = "unix"
		addr = addrRepr[5:]
	} else {
		af = "tcp"
		addr = addrRepr
	}
	return net.Listen(af, addr)
}

func NewServer(config ServerConfig) (*Server, error) {
	var acceptorListener, tunnelListener net.Listener
	var err error
	defer func() {
		if err != nil {
			if acceptorListener != nil {
				acceptorListener.Close()
			}
			if tunnelListener != nil {
				tunnelListener.Close()
			}
		}
	}()

	acceptorListener, err = doListen(config.Bind1)
	if err != nil {
		return nil, err
	}
	config.Logger.Info().Msg(fmt.Sprintf("acceptor started listening on %s", acceptorListener.Addr().String()))
	tunnelListener, err = doListen(config.Bind2)
	if err != nil {
		return nil, err
	}
	config.Logger.Info().Msg(fmt.Sprintf("tunnel started listening on %s", tunnelListener.Addr().String()))

	return &Server{
		acceptorListener:     acceptorListener,
		tunnelListener:       tunnelListener,
		logger:               config.Logger,
		acceptorClients:      make(map[net.Conn]*serverClient),
		addrToClientMap:      make(map[addrKey]*serverClient),
		tunnelClientConn:     nil,
		newClientChan:        make(chan *serverClient, 65535),
		clientCloseChan:      make(chan *serverClient, 65535),
		connReadyChan:        make(chan struct{}, 65535),
		cliClosedChan:        make(chan struct{}, 65535),
		egressChan:           make(chan *packet, 65535),
		gracefulShutdownChan: make(chan struct{}),
	}, nil
}
