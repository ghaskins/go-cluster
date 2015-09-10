package main

import (
	"flag"
	"io/ioutil"
	"fmt"
	"log"
	"encoding/pem"
	//"crypto/tls"
	"crypto/x509"
)

func main() {
	id         := flag.String("id", "localhost:2001", "our identity")
	privatekey := flag.String("key", "key1.pem", "the path to our private key")
	certspath  := flag.String("certs", "certs", "the path to our configuration")

	flag.Parse();
	fmt.Printf("id: %s, privatekey: %s, config: %s\n", *id, *privatekey, *certspath)

	certsbuf, err := ioutil.ReadFile(*certspath)
	if err != nil {
		panic("failed to open certificates file \"" + *certspath + "\"")
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

	fmt.Printf("Using %s with peers %v", self.Subject.CommonName, peers)

}
