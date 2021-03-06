package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"log"
	"strings"
)

func parseKey(path string) (crypto.PublicKey, error) {

	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.New("failed to open key file \"" + path + "\"")
	}

	block, _ := pem.Decode(buf)
	if block.Type != "PRIVATE KEY" && strings.HasSuffix(block.Type, " PRIVATE KEY") == false {
		return nil, errors.New("private key PEM does not appear to contain a private key blob")
	}

	der := block.Bytes
	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		switch key := key.(type) {
		case *rsa.PrivateKey, *ecdsa.PrivateKey:
			return key, nil
		default:
			return nil, errors.New("crypto/tls: found unknown private key type in PKCS#8 wrapping")
		}
	}
	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}

	return nil, errors.New("failed to parse private key")
}

func CreateTlsIdentity(cert *x509.Certificate, privateKeyPath string) (conn *tls.Certificate, err error) {

	var privateKey crypto.PublicKey

	privateKey, err = parseKey(privateKeyPath)
	if err != nil {
		return nil, err
	}

	tlsCert := &tls.Certificate{
		Certificate: make([][]byte, 1),
		PrivateKey:  privateKey,
	}

	tlsCert.Certificate[0] = cert.Raw

	return tlsCert, nil
}

func ParseCertificates(path string) ([]*x509.Certificate, error) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.New("failed to open certificates file \"" + path + "\"")
	}

	certs := make([]*x509.Certificate, 0)

	for remain := buf; remain != nil; {
		var block *pem.Block

		block, remain = pem.Decode(remain)
		if block == nil || block.Type != "CERTIFICATE" {
			break
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			log.Printf("Dropping certificate due to parse failure (%s)", err.Error())
			continue
		}

		if err := cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature); err != nil {
			log.Printf("Dropping certificate %s due to bad signature (%s)", cert.Subject.CommonName, err.Error())
			continue
		}

		certs = append(certs, cert)
	}

	return certs, nil
}
