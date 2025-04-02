/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import io.grpc.ServerInterceptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Metadata;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;


/**
 * Plugin that allows unauthenticated gRPC access.
 */
@DataPrepperPlugin(
        name = GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME,
        pluginType = GrpcAuthenticationProvider.class
)
public class UnauthenticatedCustomGrpcAuthenticationProvider implements GrpcAuthenticationProvider {

    @Override
    public ServerInterceptor getAuthenticationInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                // No authentication is performed; allow the request to continue
                return next.startCall(call, headers);
            }
        };
    }
}
