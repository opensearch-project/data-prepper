/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.certificate.model;

import com.google.common.io.BaseEncoding;
import org.opensearch.dataprepper.plugins.certificate.exception.CertificateFingerprintParsingException;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.MessageDigest;

import static java.util.Objects.requireNonNull;

// TODO: accommodate encrypted private key with password
public class Certificate {
    private static final String SSL_ALGORITHM = "X.509";
    private static final String RSA_ALGORITHM = "SHA-1";

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

    public String getFingerprint() {
        final X509Certificate x509Certificate = getX509Certificate();
        return convertX509CertificateToFingerprint(x509Certificate);
    }

    private X509Certificate getX509Certificate() {
        try {
            final CertificateFactory certificateFactory = CertificateFactory.getInstance(SSL_ALGORITHM);
            return (X509Certificate) certificateFactory.generateCertificate(
                    new ByteArrayInputStream(getCertificate().getBytes(StandardCharsets.UTF_8))
            );
        } catch (final CertificateException e) {
            throw new CertificateFingerprintParsingException("Unable to convert certificate to X509Certificate object", e);
        }
    }

    private String convertX509CertificateToFingerprint(final X509Certificate x509Certificate) {
        try {
            final MessageDigest messageDigest = MessageDigest.getInstance(RSA_ALGORITHM);
            final byte[] derEncodedCertificate = x509Certificate.getEncoded();
            messageDigest.update(derEncodedCertificate);
            final byte[] digest = messageDigest.digest();
            return BaseEncoding.base16().lowerCase().encode(digest);
        } catch (final NoSuchAlgorithmException | CertificateEncodingException e) {
            throw new CertificateFingerprintParsingException("Unable to convert x509Certificate to hexadecimal fingerprint", e);
        }
    }
}
