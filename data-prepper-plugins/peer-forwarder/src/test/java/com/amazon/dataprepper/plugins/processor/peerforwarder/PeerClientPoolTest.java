package com.amazon.dataprepper.plugins.processor.peerforwarder;

import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class PeerClientPoolTest {
    private static final String VALID_ADDRESS = "10.10.10.5";

    @Test
    public void testGetClientValidAddress() {
        PeerClientPool pool = PeerClientPool.getInstance();

        TraceServiceGrpc.TraceServiceBlockingStub client = pool.getClient(VALID_ADDRESS);

        assertNotNull(client);
    }
}
