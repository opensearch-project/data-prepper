/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.health;

import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;

public class HealthGrpcService extends HealthGrpc.HealthImplBase {

    @Override
    public void check(final HealthCheckRequest request, final StreamObserver<HealthCheckResponse> responseObserver) {
        responseObserver.onNext(
                HealthCheckResponse.newBuilder().setStatus(HealthCheckResponse.ServingStatus.SERVING).build());
        responseObserver.onCompleted();
    }
}
