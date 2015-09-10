package main

import (
	"net"
	"crypto/x509"
)

type Connection struct {
	conn net.Conn
	key string
}

func NewConnection(self, peer *x509.Certificate, privatekey string) *Connection {
	conn := new(Connection)
	conn.key = privatekey

	return conn
}