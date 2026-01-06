/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.auth.AuthService;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

/**
 * The plugin for gRPC Basic authentication of Armeria servers.
 *
 * @since 1.2
 */
@DataPrepperPlugin(name = "http_basic",
        pluginType = GrpcAuthenticationProvider.class,
        pluginConfigurationType = HttpBasicAuthenticationConfig.class)
public class GrpcBasicAuthenticationProvider implements GrpcAuthenticationProvider {
    private final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig;
    private final ServerInterceptor authenticationInterceptor;
    private final String base64EncodedCredentialsFromConfig;
    private static final String AUTH_HEADER = "authorization";
    private static final String BASIC = "basic";
    private static final int CREDENTIAL_START_INDEX = 5;

    @DataPrepperPluginConstructor
    public GrpcBasicAuthenticationProvider(final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig) {
        Objects.requireNonNull(httpBasicAuthenticationConfig);
        Objects.requireNonNull(httpBasicAuthenticationConfig.getUsername());
        Objects.requireNonNull(httpBasicAuthenticationConfig.getPassword());
        this.httpBasicAuthenticationConfig = httpBasicAuthenticationConfig;
        this.authenticationInterceptor = new GrpcBasicAuthenticationInterceptor();
        this.base64EncodedCredentialsFromConfig = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", httpBasicAuthenticationConfig.getUsername(), httpBasicAuthenticationConfig.getPassword()).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Optional<Function<? super HttpService, ? extends HttpService>> getHttpAuthenticationService() {
        return Optional.of(createDecorator());
    }

    private Function<? super HttpService, ? extends HttpService> createDecorator() {
        return AuthService.builder()
                .addBasicAuth((context, basic) ->
                        CompletableFuture.completedFuture(
                                httpBasicAuthenticationConfig.getUsername().equals(basic.username()) &&
                                        httpBasicAuthenticationConfig.getPassword().equals(basic.password())))
                .newDecorator();
    }

    public ServerInterceptor getAuthenticationInterceptor() {
        return authenticationInterceptor;
    }

    private class GrpcBasicAuthenticationInterceptor implements ServerInterceptor {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
            final String authorization = headers.get(Key.of(AUTH_HEADER, ASCII_STRING_MARSHALLER));
            if (authorization != null && authorization.toLowerCase().startsWith(BASIC)) {
                final String base64EncodedCredentialsFromRequestHeader = authorization.substring(CREDENTIAL_START_INDEX).trim();
                if (!base64EncodedCredentialsFromConfig.equals(base64EncodedCredentialsFromRequestHeader)) {
                    call.close(Status.UNAUTHENTICATED.withDescription("Invalid username or password\n"), headers);
                }
            } else {
                call.close(Status.UNAUTHENTICATED.withDescription("Invalid or no authorization provided\n"), headers);
            }
            return next.startCall(call, headers);
        }
    }
}

