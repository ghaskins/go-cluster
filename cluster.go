package main

import (
	"flag"
	"io/ioutil"
	"fmt"
	"log"
	"time"
	"encoding/pem"
	"crypto/tls"
	"crypto/x509"
)

func main() {
	id := flag.Int("id", 0, "the index into the certificates that corresponds to our identity")
	privateKey := flag.String("key", "key0.pem", "the path to our private key")
	certsPath := flag.String("certs", "certs", "the path to our membership definition")

	flag.Parse();
	fmt.Printf("id: %d, privatekey: %s, config: %s\n", *id, *privateKey, *certsPath)

	certsbuf, err := ioutil.ReadFile(*certsPath)
	if err != nil {
		panic("failed to open certificates file \"" + *certsPath + "\"")
	}

	certs := make([]*x509.Certificate, 0)

	for remain := certsbuf; remain != nil; {
		var block *pem.Block

		block, remain = pem.Decode(remain)
		if block == nil || block.Type != "CERTIFICATE" {
			break
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			panic(err)
		}

		if err := cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature); err != nil {
			log.Printf("Dropping certificate %s due to bad signature (%s)", cert.Subject.CommonName, err.Error())
			continue
		}

		certs = append(certs, cert)
	}

	if *id >= len(certs) {
		log.Fatalf("Invalid index")
	}

	peers       := map[string]*Identity{}
	clientPeers := map[string]*Identity{}
	serverPeers := map[string]*Identity{}

	self := NewIdentity(certs[*id])

	for i, cert := range certs {
		if i != *id {
			peer := NewIdentity(cert)
			peers[peer.Id] = peer
		}
	}

	fmt.Printf("Using %s - %s with peers:\n" , self.Cert.Subject.CommonName, self.Id)

	for _, peer := range peers {
		fmt.Printf("\t%s - %s (", peer.Cert.Subject.CommonName, peer.Id)
		if peer.Id < self.Id {
			serverPeers[peer.Id] = peer
			fmt.Printf("S")
		} else {
			clientPeers[peer.Id] = peer
			fmt.Printf("C")
		}
		fmt.Printf(")\n")
	}
	
	var tlsCert *tls.Certificate
	tlsCert, err = CreateTlsIdentity(self.Cert, *privateKey)
	if err != nil {
		panic(err)
	}

	connectionEvents := make(chan *Connection)

	// First start our primary listener if we have at least one server
	if len(serverPeers) > 0 {
		go func() {
			listener, err := Listen(tlsCert, self.Cert.Subject.CommonName)
			if err != nil {
				panic(err)
			}

			for {
				var conn *Connection
				var err error

				conn, err = Accept(listener)
				if err != nil {
					panic(err)
				}

				// Check to see if the connection is related to a peer we expect to be connecting
				// to us as a client
				if _, ok := serverPeers[conn.Id.Id]; ok {
					connectionEvents <- conn
				} else {
					log.Printf("Dropping unknown peer %v", conn.Id)
				}
			}

		}()
	}

	// Now initiate a parallel workload to form connections with any of our peers
	// that we are a client of
	for _, peer := range clientPeers {
		go func(peer Identity) {

			var conn *Connection

			for {
				var err error
				fmt.Println("Attempting to connect to " + peer.Cert.Subject.CommonName)
				conn, err = Dial(tlsCert, &peer)
				if err == nil {
					continue
				}
				time.Sleep(time.Duration(5)*time.Second)
			}

			connectionEvents <- conn
		}(*peer)
	}

	// Finally, wait for connections to come in
	for {
		conn := <-connectionEvents
		fmt.Printf("new connection from %s", conn.Id.Id)
	}
}
