/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.linecorp.armeria.client.ClientBuilder;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientFactoryBuilder;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.WebClient;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerClientPool {
    private static final String HTTP = "http";
    private static final String HTTPS = "https";
    private final Map<String, WebClient> peerClients;

    private int port;
    private int clientTimeoutSeconds = 3;
    private boolean ssl;
    private Certificate certificate;
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
            final ClientFactoryBuilder clientFactoryBuilder = ClientFactory.builder()
                    .tlsCustomizer(sslContextBuilder -> sslContextBuilder.trustManager(
                                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8))
                            )
                    )
                    .tlsNoVerifyHosts(ipAddress);
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
}
