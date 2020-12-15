package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.linecorp.armeria.client.ClientBuilder;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientFactoryBuilder;
import com.linecorp.armeria.client.Clients;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerClientPool {
    private static final PeerClientPool INSTANCE = new PeerClientPool();
    private final Map<String, TraceServiceGrpc.TraceServiceBlockingStub> peerClients;

    private int clientTimeoutSeconds = 3;
    private boolean ssl;
    private File sslKeyCertChainFile;

    private PeerClientPool() {
        peerClients = new ConcurrentHashMap<>();
    }

    public static PeerClientPool getInstance() {
        return INSTANCE;
    }

    public void setClientTimeoutSeconds(int clientTimeoutSeconds) {
        this.clientTimeoutSeconds = clientTimeoutSeconds;
    }
    public void setIsSsl(boolean ssl) {
        this.ssl = ssl;
    }
    public void setSslKeyCertChainFile(File sslKeyCertChainFile) {
        this.sslKeyCertChainFile = sslKeyCertChainFile;
    }

    public TraceServiceGrpc.TraceServiceBlockingStub getClient(final String address) {
        // TODO: Resolve to IP first, or is hostname good enough?
        return peerClients.computeIfAbsent(address, addr -> createGRPCClient(addr));
    }

    private TraceServiceGrpc.TraceServiceBlockingStub createGRPCClient(final String ipAddress) {
        // TODO: replace hardcoded port with customization
        final ClientBuilder clientBuilder = Clients.builder(String.format("gproto+http://%s:21890/", ipAddress))
                .writeTimeout(Duration.ofSeconds(clientTimeoutSeconds));
        if (ssl) {
            clientBuilder.factory(ClientFactory.builder()
                    .tlsCustomizer(sslContextBuilder -> sslContextBuilder.trustManager(sslKeyCertChainFile)).build());
        }
        return clientBuilder.build(TraceServiceGrpc.TraceServiceBlockingStub.class);
    }
}
