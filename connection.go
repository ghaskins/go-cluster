package main

import (
	"crypto/tls"
	"errors"
	"net"
	"fmt"
)

type Connection struct {
	Conn *tls.Conn
	Id *Identity
}

func verifySelfSigned(conn *tls.Conn) (*Connection, error) {

	if err := conn.Handshake(); err != nil {
		return nil, err
	}

	certs := conn.ConnectionState().PeerCertificates
	if len(certs) != 1 {
		return nil, errors.New(fmt.Sprintf("Illegal number of certificates presented by peer (%d)", len(certs)))
	}

	cert := certs[0]

	if err := cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature); err != nil {
		return nil, err
	}

	return &Connection{Conn: conn, Id: NewIdentity(cert)}, nil
}

func newConfig(self *tls.Certificate) *tls.Config {
	config := &tls.Config{
		Certificates:       make([]tls.Certificate, 1),
		InsecureSkipVerify: true,
		ClientAuth:         tls.RequireAnyClientCert,
	}

	config.Certificates[0] = *self

	return config
}

func Dial(self *tls.Certificate, peer *Identity) (conn *Connection, err error) {

	tlsConn, err := tls.Dial("tcp", peer.Cert.Subject.CommonName, newConfig(self))
	if err != nil {
		return nil, err
	}

	conn, err = verifySelfSigned(tlsConn)
	if err != nil {
		return nil, err
	}

	if conn.Id.Id != peer.Id {
		return nil, errors.New("Unexpected peer identity")
	}

	return conn, nil
}

func Listen(self *tls.Certificate, laddr string) (net.Listener, error) {
	return tls.Listen("tcp", laddr, newConfig(self))
}

func Accept(listener net.Listener) (*Connection, error) {

	conn, err := listener.Accept()
	if err != nil {
		return nil, err
	}

	return verifySelfSigned(conn.(*tls.Conn))
}