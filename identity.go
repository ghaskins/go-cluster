package main

import (
	"crypto/x509"
	"crypto/sha256"
	"fmt"
)

type Identity struct {
	Id string
	Cert *x509.Certificate
}

func NewIdentity(cert *x509.Certificate) *Identity {
	rawId := sha256.Sum256(cert.RawTBSCertificate)
	var id string

	// FIXME - convert rawId to id
	for _, val := range rawId {
		id += fmt.Sprintf("%02x", int(val))
	}

	return &Identity{Id: id, Cert: cert}
}
