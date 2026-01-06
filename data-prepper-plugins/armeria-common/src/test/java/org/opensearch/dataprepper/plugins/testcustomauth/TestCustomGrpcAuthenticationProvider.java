/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.testcustomauth;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.HttpService;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;

import java.util.Optional;
import java.util.function.Function;

@DataPrepperPlugin(
        name = "test_custom_auth",
        pluginType = GrpcAuthenticationProvider.class,
        pluginConfigurationType = TestCustomAuthenticationConfig.class
)
public class TestCustomGrpcAuthenticationProvider implements GrpcAuthenticationProvider {
    private final String token;
    private final String header;

    @DataPrepperPluginConstructor
    public TestCustomGrpcAuthenticationProvider(final TestCustomAuthenticationConfig config) {
        this.token = config.customToken();
        this.header = config.header();
    }

    @Override
    public ServerInterceptor getAuthenticationInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {

                String auth = headers.get(Metadata.Key.of(header, Metadata.ASCII_STRING_MARSHALLER));

                if (!isValid(auth)) {
                    call.close(Status.UNAUTHENTICATED.withDescription("Invalid token"), new Metadata());
                    return new ServerCall.Listener<>() {};
                }

                return next.startCall(call, headers);
            }
        };
    }

    @Override
    public Optional<Function<? super HttpService, ? extends HttpService>> getHttpAuthenticationService() {
        return Optional.of(delegate -> (ctx, req) -> {
            final String auth = req.headers().get(header);
            if (!isValid(auth)) {
                return HttpResponse.of(
                        HttpStatus.UNAUTHORIZED,
                        MediaType.PLAIN_TEXT_UTF_8,
                        "Unauthorized: Invalid or missing token"
                );
            }
            return delegate.serve(ctx, req);
        });
    }

    /**
     * Checks if the provided authentication token is valid.
     *
     * @param authHeader the value of the authentication header
     * @return true if valid, false otherwise
     */
    private boolean isValid(final String authHeader) {
        return authHeader != null && authHeader.equals(token);
    }
}


