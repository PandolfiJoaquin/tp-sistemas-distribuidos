package main

import "net"

// Peer represents any other proc different from the current one
type Peer struct {
	socket net.Conn
}
