package main

import (
	"crypto/x509"
	"crypto/tls"
	"crypto"
	//"io/ioutil"
	//"errors"
)

func parseKey(path string) (crypto.PublicKey, error) {
	/*
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.New("failed to open key file \"" + path + "\"")
	}
	*/
	//der, _ := pem.Decode(buf);

	//FIXME
	return nil, nil
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

	tlsCert.Certificate[0] = cert.RawTBSCertificate
	//tlsCert.PrivateKey[0] = privateKey

	return tlsCert, nil
}
