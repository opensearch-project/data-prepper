package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.linecorp.armeria.client.Clients;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerClientPool {
    private static final PeerClientPool INSTANCE = new PeerClientPool();
    private final Map<String, TraceServiceGrpc.TraceServiceBlockingStub> peerClients;

    private int clientTimeoutSeconds = 3;

    private PeerClientPool() {
        peerClients = new ConcurrentHashMap<>();
    }

    public static PeerClientPool getInstance() {
        return INSTANCE;
    }

    public void setClientTimeoutSeconds(int clientTimeoutSeconds) {
        this.clientTimeoutSeconds = clientTimeoutSeconds;
    }

    public TraceServiceGrpc.TraceServiceBlockingStub getClient(final String address) {
        // TODO: Resolve to IP first, or is hostname good enough?
        return peerClients.computeIfAbsent(address, addr -> createGRPCClient(addr));
    }

    private TraceServiceGrpc.TraceServiceBlockingStub createGRPCClient(final String ipAddress) {
        // TODO: replace hardcoded port with customization
        return Clients.builder(String.format("gproto+http://%s:21890/", ipAddress))
                .writeTimeout(Duration.ofSeconds(clientTimeoutSeconds))
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
    }
}
