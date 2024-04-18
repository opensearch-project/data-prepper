/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.auth.BasicToken;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import io.grpc.ServerInterceptors;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class GrpcBasicAuthenticationProviderTest {
    private static final String USERNAME = UUID.randomUUID().toString();
    private static final String PASSWORD = UUID.randomUUID().toString();

    private static GrpcAuthenticationProvider grpcAuthenticationProvider;

    @RegisterExtension
    static ServerExtension server = new ServerExtension() {
        @Override
        protected void configure(final ServerBuilder sb) {
            final HttpBasicAuthenticationConfig config = mock(HttpBasicAuthenticationConfig.class);

            when(config.getUsername()).thenReturn(USERNAME);
            when(config.getPassword()).thenReturn(PASSWORD);
            grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(config);

            final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                    .builder()
                    .enableUnframedRequests(true)
                    .addService(ServerInterceptors.intercept(new SampleHealthGrpcService(), Collections.singletonList(grpcAuthenticationProvider.getAuthenticationInterceptor())));

            sb.service(grpcServiceBuilder.build());
        }
    };

    private static class SampleHealthGrpcService extends HealthGrpc.HealthImplBase {

        @Override
        public void check(final HealthCheckRequest request, final StreamObserver<HealthCheckResponse> responseObserver) {
            responseObserver.onNext(
                    HealthCheckResponse.newBuilder().setStatus(HealthCheckResponse.ServingStatus.SERVING).build());
            responseObserver.onCompleted();
        }
    }

    @Nested
    class ConstructorTests {
        private HttpBasicAuthenticationConfig config;

        @BeforeEach
        void setUp() {
            config = mock(HttpBasicAuthenticationConfig.class);

        }

        private GrpcBasicAuthenticationProvider createObjectUnderTest() {
            return new GrpcBasicAuthenticationProvider(config);
        }

        @Test
        void constructor_with_null_Config_throws() {
            config = null;
            assertThrows(NullPointerException.class, this::createObjectUnderTest);
        }

        @Test
        void constructor_with_null_username_throws() {
            reset(config);
            when(config.getPassword()).thenReturn(UUID.randomUUID().toString());
            assertThrows(NullPointerException.class, this::createObjectUnderTest);
        }

        @Test
        void constructor_with_null_password_throws() {
            reset(config);
            when(config.getUsername()).thenReturn(UUID.randomUUID().toString());
            assertThrows(NullPointerException.class, this::createObjectUnderTest);
        }
    }

    @Nested
    class WithServer {
        @Test
        void httpRequest_without_authentication_responds_Unauthorized() {
            final WebClient client = WebClient.of(server.httpUri());

            HttpRequest request = HttpRequest.of(RequestHeaders.builder()
                    .method(HttpMethod.POST)
                    .path("/grpc.health.v1.Health/Check")
                    .contentType(MediaType.JSON_UTF_8)
                    .build());

            final AggregatedHttpResponse httpResponse = client.execute(request).aggregate().join();
            assertThat(httpResponse.status(), equalTo(HttpStatus.UNAUTHORIZED));
        }

        @Test
        void httpRequest_with_incorrect_authentication_responds_Unauthorized() {
            final WebClient client = WebClient.builder(server.httpUri())
                    .auth(BasicToken.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                    .build();

            HttpRequest request = HttpRequest.of(RequestHeaders.builder()
                    .method(HttpMethod.POST)
                    .path("/grpc.health.v1.Health/Check")
                    .contentType(MediaType.JSON_UTF_8)
                    .build());

            final AggregatedHttpResponse httpResponse = client.execute(request).aggregate().join();

            assertThat(httpResponse.status(), equalTo(HttpStatus.UNAUTHORIZED));
        }

        @Test
        void httpRequest_with_correct_authentication_responds_OK() {
            final WebClient client = WebClient.builder(server.httpUri())
                    .auth(BasicToken.of(USERNAME, PASSWORD))
                    .build();

            HttpRequest request = HttpRequest.of(RequestHeaders.builder()
                    .method(HttpMethod.POST)
                    .path("/grpc.health.v1.Health/Check")
                    .contentType(MediaType.JSON_UTF_8)
                    .build(),
                    HttpData.of(Charset.defaultCharset(), "{\"healthCheckConfig\":{\"serviceName\": \"test\"} }"));

            final AggregatedHttpResponse httpResponse = client.execute(request).aggregate().join();

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));
        }
    }
}
