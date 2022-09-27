/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.google.common.io.BaseEncoding;
import com.linecorp.armeria.client.ClientBuilder;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientFactoryBuilder;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.WebClient;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.peerforwarder.exception.CertificateFingerprintParsingException;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.MessageDigest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerClientPool {
    private static final String HTTP = "http";
    private static final String HTTPS = "https";
    private static final String SSL_ALGORITHM = "X.509";
    private static final String RSA_ALGORITHM = "SHA-1";
    private final Map<String, WebClient> peerClients;

    private int port;
    private int clientTimeoutSeconds = 3;
    private boolean ssl;
    private Certificate certificate;
    private boolean sslDisableVerification;
    private boolean sslFingerprintVerificationOnly;
    private ForwardingAuthentication authentication;

    public PeerClientPool() {
        peerClients = new ConcurrentHashMap<>();
    }

    public void setClientTimeoutSeconds(int clientTimeoutSeconds) {
        this.clientTimeoutSeconds = clientTimeoutSeconds;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setCertificate(final Certificate certificate) {
        this.certificate = certificate;
    }

    public void setSslDisableVerification(final boolean sslDisableVerification) {
        this.sslDisableVerification = sslDisableVerification;
    }

    public void setSslFingerprintVerificationOnly(final boolean sslFingerprintVerificationOnly) {
        this.sslFingerprintVerificationOnly = sslFingerprintVerificationOnly;
    }

    public void setAuthentication(ForwardingAuthentication authentication) {
        this.authentication = authentication;
    }

    public WebClient getClient(final String address) {
        return peerClients.computeIfAbsent(address, this::getHTTPClient);
    }

    private WebClient getHTTPClient(final String ipAddress) {
        final String protocol = ssl ? HTTPS : HTTP;

        ClientBuilder clientBuilder = Clients.builder(String.format("%s://%s:%s/", protocol, ipAddress, port))
                .writeTimeout(Duration.ofSeconds(clientTimeoutSeconds));

        if (ssl) {
            final ClientFactoryBuilder clientFactoryBuilder = ClientFactory.builder();

            if (sslFingerprintVerificationOnly) {
                final FingerprintTrustManagerFactory fingerprintTrustManagerFactory = new FingerprintTrustManagerFactory(getCertificateFingerPrint());
                clientFactoryBuilder.tlsCustomizer(sslContextBuilder -> sslContextBuilder.trustManager(fingerprintTrustManagerFactory));
            } else {
                clientFactoryBuilder.tlsCustomizer(sslContextBuilder -> sslContextBuilder.trustManager(
                                new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8))
                        )
                );
            }

            if(sslDisableVerification) {
                clientFactoryBuilder.tlsNoVerifyHosts(ipAddress);
            }

            // TODO: Add keyManager configuration here
            if (authentication == ForwardingAuthentication.MUTUAL_TLS) {
                clientFactoryBuilder.tlsCustomizer(sslContextBuilder -> sslContextBuilder.keyManager(
                        new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                        new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8))
                ));
            }
            clientBuilder = clientBuilder.factory(clientFactoryBuilder.build());
        }

        return clientBuilder.build(WebClient.class);
    }

    private String getCertificateFingerPrint() {
        final X509Certificate x509Certificate = getX509Certificate();
        return convertX509CertificateToFingerprint(x509Certificate);
    }

    private X509Certificate getX509Certificate() {
        try {
            final CertificateFactory certificateFactory = CertificateFactory.getInstance(SSL_ALGORITHM);
            return (X509Certificate) certificateFactory.generateCertificate(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8))
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
