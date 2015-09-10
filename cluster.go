package main

import (
	"flag"
	"io/ioutil"
	"fmt"
	"log"
	"encoding/pem"
	"crypto/tls"
	"crypto/x509"
)

func main() {
	id         := flag.String("id", "localhost:2001", "our identity")
	privateKey := flag.String("key", "key1.pem", "the path to our private key")
	certsPath  := flag.String("certs", "certs", "the path to our configuration")

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

	peers := make([]*x509.Certificate, 0)
	var self *x509.Certificate

	for _, cert := range certs {
		if cert.Subject.CommonName != *id {
			peers = append(peers, cert)
		} else {
			self = cert
		}
	}

	fmt.Printf("Using %s with peers %v\n", self.Subject.CommonName, peers)

	var tlsCert *tls.Certificate
	tlsCert, err = CreateTlsIdentity(self, *privateKey)
	if err != nil {
		panic(err)
	}

	for _, peer := range peers {
		var conn *tls.Conn
		conn, err = Dial(tlsCert, peer)

		fmt.Println(conn)
	}
}
