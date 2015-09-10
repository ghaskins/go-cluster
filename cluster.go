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
	id         := flag.String("id", "localhost:2001", "our identity")
	privateKey := flag.String("key", "key1.pem", "the path to our private key")
	certsPath  := flag.String("certs", "certs", "the path to our membership definition")

	flag.Parse();
	fmt.Printf("id: %s, privatekey: %s, config: %s\n", *id, *privateKey, *certsPath)

	certsbuf, err := ioutil.ReadFile(*certsPath)
	if err != nil {
		panic("failed to open certificates file \"" + *certsPath + "\"")
	}

	certs := make([]*x509.Certificate, 0)

	for remain := certsbuf; remain != nil; {
		var block *pem.Block

		block, remain = pem.Decode(remain)
		if block == nil || block.Type != "CERTIFICATE"{
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

	peers := map[string]*Identity{}
	var self *Identity

	for _, cert := range certs {
		if cert.Subject.CommonName != *id {
			peer := NewIdentity(cert)
			peers[peer.Id] = peer
		} else {
			self = NewIdentity(cert)
		}
	}

	fmt.Printf("Using %s with peers %v\n", self.Cert.Subject.CommonName, peers)

	var tlsCert *tls.Certificate
	tlsCert, err = CreateTlsIdentity(self.Cert, *privateKey)
	if err != nil {
		panic(err)
	}

	connectionEvents := make(chan *Connection)

	// First start our primary listener
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

			// Check to see if the connection is related to a peer we expect
			if _, ok := peers[conn.Id.Id]; ok {
				connectionEvents <- conn
			} else {
				log.Printf("Dropping unknown peer %v", conn.Id)
			}
		}

	}()

	// Now initiate a parallel workload to form connections with any of our peers
	// that have an ID greater than our own
	for _, peer := range peers {
		if peer.Id > self.Id {
			go func() {

				var conn *Connection

				for {
					var err error
					conn, err = Dial(tlsCert, peer)
					if err == nil {
						continue
					}
					time.Sleep(time.Duration(5)*time.Second)
				}

				connectionEvents <- conn
			}()
		}
	}

	for {
		conn := <-connectionEvents
		fmt.Printf("new connection from %s", conn.Id.Id)
	}
}
