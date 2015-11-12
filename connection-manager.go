package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"time"
)

type ConnectionManager struct {
	id      *Identity
	cert    *tls.Certificate
	peers   IdentityMap
	servers IdentityMap
	clients IdentityMap
	C       chan *Connection
}

func NewConnectionManager(_id *Identity, _cert *tls.Certificate, _peers IdentityMap) *ConnectionManager {
	self := &ConnectionManager{
		id:      _id,
		cert:    _cert,
		peers:   _peers,
		servers: IdentityMap{},
		clients: IdentityMap{},
		C:       make(chan *Connection, 100),
	}

	fmt.Printf("Using %s - %s with peers:\n", self.id.Cert.Subject.CommonName, self.id.Id)

	for _, peer := range _peers {
		fmt.Printf("\t%s - %s (", peer.Cert.Subject.CommonName, peer.Id)
		if peer.Id < self.id.Id {
			self.servers[peer.Id] = peer
			fmt.Printf("S")
		} else {
			self.clients[peer.Id] = peer
			fmt.Printf("C")
		}
		fmt.Printf(")\n")
	}

	// First start our primary listener if we have at least one client of our server
	if len(self.servers) > 0 {
		go func() {
			listener, err := Listen(self.cert, self.id.Cert.Subject.CommonName)
			if err != nil {
				panic(err)
			}

			for {
				var conn *Connection
				var err error

				conn, err = Accept(listener)
				if err != nil {
					log.Printf("Dropping connection: %s", err.Error())
					continue
				}

				// Check to see if the connection is related to a peer we expect to be connecting
				// to us as a client
				if _, ok := self.servers[conn.Id.Id]; ok {
					self.C <- conn
				} else {
					log.Printf("Dropping unknown peer %v", conn.Id)
				}
			}

		}()
	}

	// Now initiate a parallel workload to form connections with any of our peers
	// that we are a client of
	for _, peer := range self.clients {
		self.Dial(peer.Id)
	}

	return self
}

func (self *ConnectionManager) Dial(peerId string) {
	peer, ok := self.clients[peerId]
	if ok == false {
		// We only redial peers in the "client" category
		return
	}

	go func() {

		var conn *Connection

		for {
			var err error
			conn, err = Dial(self.cert, peer)
			if err == nil {
				break
			}
			time.Sleep(time.Duration(5) * time.Second)
		}

		self.C <- conn
	}()
}
