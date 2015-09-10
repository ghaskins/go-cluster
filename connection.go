package main

import (
	"encoding/pem"
	"crypto/x509"
	"crypto/tls"
	"crypto"
	"io/ioutil"
	"errors"
)

func verifySelfSigned(conn *tls.Conn) error {
	certs := conn.ConnectionState().PeerCertificates

	if len(certs) != 1 {
		return errors.New("Illegal number of certificates presented by peer (" + string(len(certs)) + ")")
	}

	cert := certs[0]

	return cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature)
}

func newConfig(self *tls.Certificate) *tls.Config {
	config := &tls.Config{
		Certificates:       make([]tls.Certificate, 1),
		InsecureSkipVerify: true,
	}

	config.Certificates[0] = self

	return config
}

func Dial(self *tls.Certificate, peer *x509.Certificate) (conn *tls.Conn, err error) {
	config := newConfig(self)

	if conn, err = tls.Dial("tcp", peer.Subject.CommonName, config); err != nil {
		return nil, err
	}

	if err = verifySelfSigned(conn) {
		return nil, err
	}

	return conn, nil
}