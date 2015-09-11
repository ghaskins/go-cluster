package main

import (
	"crypto/tls"
	"errors"
	"net"
	"fmt"
	"github.com/golang/protobuf/proto"
	"encoding/binary"
)

type Connection struct {
	Conn *tls.Conn
	Id *Identity
}

func (c *Connection) Send(m proto.Message) error {
	msg, err := proto.Marshal(m)
	if err != nil {
		return err
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(msg)))
	c.Conn.Write(header)
	c.Conn.Write(msg)

	return nil
}

func (c *Connection) Recv(m proto.Message) error {
	header := make([]byte, 4)
	hLen, err := c.Conn.Read(header)
	if err != nil || hLen != 4 {
		return err
	}

	len := binary.BigEndian.Uint32(header)
	// FIXME: guard against an upper MTU violation
	payload := make([]byte, len)
	err = proto.Unmarshal(payload, m)
	if err != nil {
		return err
	}

	return nil
}

func verifyCrypto(conn *tls.Conn) (*Connection, error) {

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

func newNegotiate() *Negotiate {
	return &Negotiate{
		Magic:   proto.String("cluster"),
		Version: proto.Int(1),
	}
}

func Dial(self *tls.Certificate, peer *Identity) (conn *Connection, err error) {

	tlsConn, err := tls.Dial("tcp", peer.Cert.Subject.CommonName, newConfig(self))
	if err != nil {
		return nil, err
	}

	conn, err = verifyCrypto(tlsConn)
	if err != nil {
		return nil, err
	}

	if conn.Id.Id != peer.Id {
		return nil, errors.New("Unexpected peer identity")
	}

	// Negotiation protocol: send a Negotiate packet to the server, and wait for
	// a response.  Then ensure baseline compatibility
	ours := newNegotiate()
	theirs := &Negotiate{}

	conn.Send(ours)
	err = conn.Recv(theirs)
	if err != nil {
		return nil, err
	}

	if ours.Magic != theirs.Magic || ours.Version != theirs.Version {
		return nil, errors.New("incompatible wire protocol")
	}

	return conn, nil
}

func Listen(self *tls.Certificate, laddr string) (net.Listener, error) {
	return tls.Listen("tcp", laddr, newConfig(self))
}

func Accept(listener net.Listener) (*Connection, error) {

	tlsConn, err := listener.Accept()
	if err != nil {
		return nil, err
	}

	conn, err := verifyCrypto(tlsConn.(*tls.Conn))
	if err != nil {
		return nil, err
	}

	// Negotiation protocol: wait for a negotiate message, compare, and reply
	ours := newNegotiate()
	theirs := &Negotiate{}

	err = conn.Recv(theirs)
	if err != nil {
		return nil, err
	}

	if ours.Magic != theirs.Magic || ours.Version != theirs.Version {
		return nil, errors.New("incompatible wire protocol")
	}

	conn.Send(ours)

	return conn, nil
}