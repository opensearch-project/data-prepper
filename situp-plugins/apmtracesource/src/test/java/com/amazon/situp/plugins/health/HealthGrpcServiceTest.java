package com.amazon.situp.plugins.health;

import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;

public class HealthGrpcServiceTest {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Test
    void testHealthCheckResponse() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        grpcCleanup.register(
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(new HealthGrpcService())
                        .build()
                        .start());

        HealthGrpc.HealthBlockingStub blockingStub = HealthGrpc.newBlockingStub(
                grpcCleanup.register(
                        InProcessChannelBuilder.forName(serverName)
                                .directExecutor()
                                .build()));

        HealthCheckResponse response =
                blockingStub.check(HealthCheckRequest.newBuilder().build());

        assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
    }
}
