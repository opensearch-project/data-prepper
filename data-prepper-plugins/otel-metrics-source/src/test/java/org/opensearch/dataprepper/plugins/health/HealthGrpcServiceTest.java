/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.health;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;


public class HealthGrpcServiceTest {

    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    public void setup() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new HealthGrpcService())
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();
    }

    @AfterEach
    public void teardown() throws Exception {
        try {
            channel.shutdown();
            server.shutdown();

            assert channel.awaitTermination(10, TimeUnit.SECONDS) : "channel cannot be gracefully shutdown";
            assert server.awaitTermination(10, TimeUnit.SECONDS) : "server cannot be gracefully shutdown";
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void testHealthCheckResponse() {
        HealthGrpc.HealthBlockingStub blockingStub = HealthGrpc.newBlockingStub(channel);
        HealthCheckResponse response =
                blockingStub.check(HealthCheckRequest.newBuilder().build());

        Assert.assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
    }
}
