package com.amazon.situp.plugins.health;

import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;

public class HealthGrpcService extends HealthGrpc.HealthImplBase {

    @Override
    public void check(final HealthCheckRequest request, final StreamObserver<HealthCheckResponse> responseObserver) {
        if (isHealthy()) {
            responseObserver.onNext(
                    HealthCheckResponse.newBuilder().setStatus(HealthCheckResponse.ServingStatus.SERVING).build());
        } else {
            responseObserver.onNext(
                    HealthCheckResponse.newBuilder().setStatus(HealthCheckResponse.ServingStatus.NOT_SERVING).build());
        }

        responseObserver.onCompleted();
    }

    private boolean isHealthy() {
        return true;
    }
}
