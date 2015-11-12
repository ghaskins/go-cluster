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

	self := NewIdentity(certs[*id])

	members := IdentityMap{}
	peers := IdentityMap{}

	for _, cert := range certs {
		member := NewIdentity(cert)
		members[member.Id] = member
		peers[member.Id] = member
	}

	delete(peers, self.Id) // peers are all members _except_ ourselves

	var tlsCert *tls.Certificate
	tlsCert, err = CreateTlsIdentity(self.Cert, *privateKey)
	if err != nil {
		panic(err)
	}

	connMgr := NewConnectionManager(self, tlsCert, peers)
	controller := NewController(self.Id, members, connMgr)

	controller.Run()
}
