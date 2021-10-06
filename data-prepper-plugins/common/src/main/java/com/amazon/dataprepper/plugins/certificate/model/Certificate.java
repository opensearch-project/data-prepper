package com.amazon.dataprepper.plugins.certificate.model;

import static java.util.Objects.requireNonNull;

// TODO: accommodate encrypted private key with password
public class Certificate {
    /**
     * The base64 PEM-encoded certificate.
     */
    private String certificate;

    /**
     * The decrypted private key associated with the public key in the certificate. The key is output in PKCS #8 format
     * and is base64 PEM-encoded.
     */
    private String privateKey;

    public Certificate(final String certificate, final String privateKey) {
        this.certificate = requireNonNull(certificate, "certificate must not be null");
        this.privateKey = requireNonNull(privateKey, "privateKey must not be null");
    }

    public String getCertificate() {
        return certificate;
    }

    public String getPrivateKey() {
        return privateKey;
    }
}
