package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
)

type IdentityMap map[string]*Identity

func main() {
	id := flag.Int("id", 0, "the index into the certificates that corresponds to our identity")
	privateKey := flag.String("key", "key0.pem", "the path to our private key")
	certsPath := flag.String("certs", "certs.conf", "the path to our membership definition")

	flag.Parse()
	fmt.Printf("id: %d, privatekey: %s, config: %s\n", *id, *privateKey, *certsPath)

	certs, err := ParseCertificates(*certsPath)
	if err != nil {
		panic(err)
	}

	if *id >= len(certs) {
		log.Fatalf("Invalid index")
	}

	allMembers := IdentityMap{}
	allPeers := IdentityMap{}

	self := NewIdentity(certs[*id])

	for i, cert := range certs {
		member := NewIdentity(cert)
		allMembers[member.Id] = member
		if i != *id {
			allPeers[member.Id] = member
		}
	}

	fmt.Printf("Using %s - %s with peers:\n", self.Cert.Subject.CommonName, self.Id)

	var tlsCert *tls.Certificate
	tlsCert, err = CreateTlsIdentity(self.Cert, *privateKey)
	if err != nil {
		panic(err)
	}

	connMgr := NewConnectionManager(self, tlsCert, allPeers)
	controller := NewController(self.Id, allMembers, connMgr)

	controller.Run()
}
