/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.HttpService;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.opensearch.dataprepper.armeria.authentication.CustomAuthenticationConfig;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;

import java.util.Optional;
import java.util.function.Function;

@DataPrepperPlugin(
        name = "custom_auth",
        pluginType = GrpcAuthenticationProvider.class,
        pluginConfigurationType = CustomAuthenticationConfig.class
)
public class CustomGrpcAuthenticationProvider implements GrpcAuthenticationProvider {
    private final String token;
    private static final String AUTH_HEADER = "authentication";


    @DataPrepperPluginConstructor
    public CustomGrpcAuthenticationProvider(final CustomAuthenticationConfig config) {
        this.token = config.customToken();
    }

    @Override
    public ServerInterceptor getAuthenticationInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {

                String auth = headers.get(Metadata.Key.of("authentication", Metadata.ASCII_STRING_MARSHALLER));

                if (auth == null || !auth.equals(token)) {
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
            final String auth = req.headers().get(AUTH_HEADER);
            if (auth == null || !auth.equals(token)) {
                return HttpResponse.of(
                        HttpStatus.UNAUTHORIZED,
                        MediaType.PLAIN_TEXT_UTF_8,
                        "Unauthorized: Invalid or missing token"
                );
            }
            return delegate.serve(ctx, req);
        });
    }
}


