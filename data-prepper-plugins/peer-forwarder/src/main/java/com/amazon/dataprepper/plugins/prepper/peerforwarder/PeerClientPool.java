package com.amazon.dataprepper.plugins.prepper.peerforwarder;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;
import com.linecorp.armeria.client.ClientBuilder;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.Clients;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerClientPool {
    private static final String GRPC_HTTP = "gproto+http";
    private static final String GRPC_HTTPS = "gproto+https";
    private static final PeerClientPool INSTANCE = new PeerClientPool();
    private final Map<String, TraceServiceGrpc.TraceServiceBlockingStub> peerClients;

    private int clientTimeoutSeconds = 3;
    private boolean ssl;
    private Certificate certificate;

    private PeerClientPool() {
        peerClients = new ConcurrentHashMap<>();
    }

    public static PeerClientPool getInstance() {
        return INSTANCE;
    }

    public void setClientTimeoutSeconds(int clientTimeoutSeconds) {
        this.clientTimeoutSeconds = clientTimeoutSeconds;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public void setCertificate(final Certificate certificate) {
        this.certificate = certificate;
    }

    public TraceServiceGrpc.TraceServiceBlockingStub getClient(final String address) {
        // TODO: Resolve to IP first, or is hostname good enough?
        return peerClients.computeIfAbsent(address, addr -> createGRPCClient(addr));
    }

    private TraceServiceGrpc.TraceServiceBlockingStub createGRPCClient(final String ipAddress) {
        // TODO: replace hardcoded port with customization
        final ClientBuilder clientBuilder;
        if (ssl) {
            clientBuilder = Clients.builder(String.format("%s://%s:21890/", GRPC_HTTPS, ipAddress))
                    .writeTimeout(Duration.ofSeconds(clientTimeoutSeconds))
                    .factory(ClientFactory.builder()
                            .tlsCustomizer(sslContextBuilder -> sslContextBuilder.trustManager(
                                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8))
                                    )
                            ).tlsNoVerifyHosts(ipAddress)
                            .build()
                    );
        } else {
            clientBuilder = Clients.builder(String.format("%s://%s:21890/", GRPC_HTTP, ipAddress))
                    .writeTimeout(Duration.ofSeconds(clientTimeoutSeconds));
        }

        return clientBuilder.build(TraceServiceGrpc.TraceServiceBlockingStub.class);
    }
}
