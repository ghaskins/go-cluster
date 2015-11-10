package main

import (
	"crypto/sha256"
	"crypto/x509"
	"fmt"
)

type Identity struct {
	Id   string
	Cert *x509.Certificate
}

func NewIdentity(cert *x509.Certificate) *Identity {
	rawId := sha256.Sum256(cert.RawTBSCertificate)
	var id string

	for _, val := range rawId {
		id += fmt.Sprintf("%02x", int(val))
	}

	return &Identity{Id: id, Cert: cert}
}
