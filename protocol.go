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
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
)

const protocolVersion = 5
const cmdSuccess = 0
const cmdConnect = 1
const cmdSend = 4
const cmdCloseNotify = 5
const cmdConnClose = 254
const cmdPacket = 255
const errGeneralFailure = 1
const errNetworkUnreachable = 3
const errHostUnreachable = 4
const errConnRefused = 5
const addrTypeV4 = 1
const addrTypeV6 = 4

func typeCodeForAddr(ip net.IP) (int, net.IP) {
	if canonIP := ip.To4(); canonIP != nil {
		return addrTypeV4, canonIP
	} else if canonIP := ip.To16(); canonIP != nil {
		return addrTypeV6, canonIP
	} else {
		panic("something goes really wrong")
	}
}

func translateToErrorCode(err error) int {
	if err, ok := err.(*os.SyscallError); ok {
		if errno, ok := err.Err.(syscall.Errno); ok {
			switch errno {
			case syscall.ECONNREFUSED:
				return errConnRefused
			case syscall.ENETUNREACH:
				return errNetworkUnreachable
			case syscall.EHOSTUNREACH:
				return errHostUnreachable
			}
		}
	}
	return errGeneralFailure
}

func sendPacket(dest net.Conn, cmd int, addr net.TCPAddr, payload []byte) error {
	atyp, canonIP := typeCodeForAddr(addr.IP)
	b := make([]byte, 4, 4+16+2+4+len(payload))
	b[0] = protocolVersion
	b[1] = byte(cmd)
	b[3] = byte(atyp)
	b = append(b, canonIP...)
	b = append(b, byte(addr.Port>>8), byte(addr.Port))
	b = append(b, byte(len(payload)>>24), byte(len(payload)>>16), byte(len(payload)>>8), byte(len(payload)))
	b = append(b, payload...)
	n, err := dest.Write(b)
	if err != nil {
		return fmt.Errorf("error occurred during sending a packet: %w", err)
	}
	if len(b) != n {
		return fmt.Errorf("failed to write %d bytes (%d bytes actual)", len(b), n)
	}
	return nil
}

type addrKey struct {
	IP   [16]byte
	Port int
}

func (k *addrKey) ToTCPAddr() net.TCPAddr {
	return net.TCPAddr{
		IP:   k.IP[:],
		Port: k.Port,
	}
}

func (k *addrKey) String() string {
	tcpAddr := k.ToTCPAddr()
	return tcpAddr.String()
}

func recvReplyOrPacket(src net.Conn, cmd *int, addr *addrKey, payload *[]byte) error {
	b := make([]byte, 4, 4+16+2+4)
	n, err := io.ReadFull(src, b)
	if err != nil {
		if os.IsTimeout(err) || err == io.EOF {
			return err
		}
		return fmt.Errorf("error occurred during receiving a reply packet: %w", err)
	}
	if n != len(b) {
		return fmt.Errorf("failed to read %d bytes (%d bytes actual)", len(b), n)
	}
	if b[0] != protocolVersion {
		return fmt.Errorf("protocol version mismatch: expected %d, %d actual", protocolVersion, int(b[0]))
	}
	*cmd = int(b[1])
	switch int(b[3]) {
	case addrTypeV4:
		n, err := io.ReadFull(src, b[4:10])
		if err != nil {
			if os.IsTimeout(err) || err == io.EOF {
				return err
			}
			return fmt.Errorf("error occurred during receiving a reply packet: %w", err)
		}
		if n != 6 {
			return fmt.Errorf("failed to read %d bytes (%d bytes actual)", 6, n)
		}
		b = b[:10]
		addr.IP[0] = 0
		addr.IP[1] = 0
		addr.IP[2] = 0
		addr.IP[3] = 0
		addr.IP[4] = 0
		addr.IP[5] = 0
		addr.IP[6] = 0
		addr.IP[7] = 0
		addr.IP[8] = 0
		addr.IP[9] = 0
		addr.IP[10] = 255
		addr.IP[11] = 255
		copy(addr.IP[12:16], b[4:8])
		addr.Port = (int(b[8]) << 8) | int(b[9])
	case addrTypeV6:
		n, err := io.ReadFull(src, b[4:22])
		if err != nil {
			if os.IsTimeout(err) || err == io.EOF {
				return err
			}
			return fmt.Errorf("error occurred during receiving a reply packet: %w", err)
		}
		if n != 18 {
			return fmt.Errorf("failed to read %d bytes (%d bytes actual)", 18, n)
		}
		b = b[:22]
		copy(addr.IP[:], b[4:20])
		addr.Port = (int(b[20]) << 8) | int(b[21])
	default:
		return fmt.Errorf("unsupported address type: %d", int(b[3]))
	}
	if b[1] == cmdPacket {
		n, err := io.ReadFull(src, b[len(b):len(b)+4])
		if err != nil {
			if os.IsTimeout(err) || err == io.EOF {
				return err
			}
			return fmt.Errorf("error occurred during receiving a reply packet: %w", err)
		}
		if n != 4 {
			return fmt.Errorf("failed to read %d bytes (%d bytes actual)", 4, n)
		}
		b = b[len(b) : len(b)+4]
		nb := (int(b[0]) << 24) | (int(b[1]) << 16) | (int(b[2]) << 8) | int(b[3])
		*payload = make([]byte, nb)
		n, err = io.ReadFull(src, *payload)
		if err != nil {
			if os.IsTimeout(err) || err == io.EOF {
				return err
			}
			return fmt.Errorf("error occurred during receiving a reply packet: %w", err)
		}
		if n != nb {
			return fmt.Errorf("failed to read %d bytes (%d bytes actual)", nb, n)
		}
	}
	return nil
}

func recvRequestOrPacket(src net.Conn, cmd *int, addr *addrKey, payload *[]byte) error {
	b := make([]byte, 4, 4+16+2+4)
	n, err := io.ReadFull(src, b)
	if err != nil {
		if os.IsTimeout(err) || err == io.EOF {
			return err
		}
		return fmt.Errorf("error occurred during receiving a packet: %w", err)
	}
	if n != len(b) {
		return fmt.Errorf("failed to read %d bytes (%d bytes actual)", len(b), n)
	}
	if b[0] != protocolVersion {
		return fmt.Errorf("protocol version mismatch: expected %d, %d actual", protocolVersion, int(b[0]))
	}
	*cmd = int(b[1])
	switch int(b[3]) {
	case addrTypeV4:
		n, err := io.ReadFull(src, b[4:10])
		if err != nil {
			if os.IsTimeout(err) || err == io.EOF {
				return err
			}
			return fmt.Errorf("error occurred during receiving a packet: %w", err)
		}
		if n != 6 {
			return fmt.Errorf("failed to read %d bytes (%d bytes actual)", 6, n)
		}
		b = b[:10]
		addr.IP[0] = 0
		addr.IP[1] = 0
		addr.IP[2] = 0
		addr.IP[3] = 0
		addr.IP[4] = 0
		addr.IP[5] = 0
		addr.IP[6] = 0
		addr.IP[7] = 0
		addr.IP[8] = 0
		addr.IP[9] = 0
		addr.IP[10] = 255
		addr.IP[11] = 255
		copy(addr.IP[12:16], b[4:8])
		addr.Port = (int(b[8]) << 8) | int(b[9])
	case addrTypeV6:
		n, err := io.ReadFull(src, b[4:22])
		if err != nil {
			if os.IsTimeout(err) || err == io.EOF {
				return err
			}
			return fmt.Errorf("error occurred during receiving a packet: %w", err)
		}
		if n != 18 {
			return fmt.Errorf("failed to read %d bytes (%d bytes actual)", 18, n)
		}
		b = b[:22]
		copy(addr.IP[:], b[4:20])
		addr.Port = (int(b[20]) << 8) | int(b[21])
	default:
		return fmt.Errorf("unsupported address type: %d", int(b[3]))
	}
	switch b[1] {
	case cmdConnect, cmdCloseNotify:
		break
	case cmdSend:
		n, err := io.ReadFull(src, b[len(b):len(b)+4])
		if err != nil {
			if os.IsTimeout(err) || err == io.EOF {
				return err
			}
			return fmt.Errorf("error occurred during receiving a packet: %w", err)
		}
		if n != 4 {
			return fmt.Errorf("failed to read %d bytes (%d bytes actual)", 4, n)
		}
		b = b[len(b) : len(b)+4]
		nb := (int(b[0]) << 24) | (int(b[1]) << 16) | (int(b[2]) << 8) | int(b[3])
		*payload = make([]byte, nb)
		n, err = io.ReadFull(src, *payload)
		if err != nil {
			if os.IsTimeout(err) || err == io.EOF {
				return err
			}
			return fmt.Errorf("error occurred during receiving a packet: %w", err)
		}
		if n != nb {
			return fmt.Errorf("failed to read %d bytes (%d bytes actual)", nb, n)
		}
	default:
		return fmt.Errorf("counterpart sent an unknown command: %d", int(b[1]))
	}
	return nil
}

func sendCommand(dest net.Conn, cmd int, addr *addrKey) error {
	atyp, canonIP := typeCodeForAddr(addr.IP[:])
	b := make([]byte, 4, 4+16+2)
	b[0] = protocolVersion
	b[1] = byte(cmd)
	b[3] = byte(atyp)
	b = append(b, canonIP...)
	b = append(b, byte(addr.Port>>8), byte(addr.Port))
	n, err := dest.Write(b)
	if err != nil {
		return fmt.Errorf("error occured during sending a command: %w", err)
	}
	if len(b) != n {
		return fmt.Errorf("failed to write %d bytes (%d bytes actual)", len(b), n)
	}
	return nil
}

func addrKeyFromTCPAddr(addr *net.TCPAddr) addrKey {
	var k addrKey
	copy(k.IP[:], addr.IP)
	k.Port = addr.Port
	return k
}
