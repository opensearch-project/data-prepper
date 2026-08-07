/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.testcustomauth;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
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

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCustomBasicAuthenticationProviderTest {
    private static final String TOKEN = UUID.randomUUID().toString();
    private static final String HEADER_NAME = "x-" + UUID.randomUUID();
    private static GrpcAuthenticationProvider grpcAuthenticationProvider;

    @RegisterExtension
    static ServerExtension server = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            TestCustomAuthenticationConfig config = mock(TestCustomAuthenticationConfig.class);
            when(config.customToken()).thenReturn(TOKEN);
            when(config.header()).thenReturn(HEADER_NAME);

            grpcAuthenticationProvider = new TestCustomGrpcAuthenticationProvider(config);

            GrpcServiceBuilder grpcServiceBuilder = GrpcService.builder()
                    .enableUnframedRequests(true)
                    .addService(ServerInterceptors.intercept(
                            new SampleHealthGrpcService(),
                            Collections.singletonList(grpcAuthenticationProvider.getAuthenticationInterceptor())));

            sb.service(grpcServiceBuilder.build());
        }
    };

    private static class SampleHealthGrpcService extends HealthGrpc.HealthImplBase {
        @Override
        public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
            responseObserver.onNext(
                    HealthCheckResponse.newBuilder().setStatus(HealthCheckResponse.ServingStatus.SERVING).build());
            responseObserver.onCompleted();
        }
    }

    @Nested
    class ConstructorTests {
        TestCustomAuthenticationConfig config;

        @BeforeEach
        void setUp() {
            config = mock(TestCustomAuthenticationConfig.class);
        }

        @Test
        void constructor_with_null_config_throws() {
            assertThrows(NullPointerException.class, () -> new TestCustomGrpcAuthenticationProvider(null));
        }
    }

    @Nested
    class WithServer {
        @Test
        void request_without_token_responds_Unauthorized() {
            WebClient client = WebClient.of(server.httpUri());
            HttpRequest request = HttpRequest.of(RequestHeaders.builder()
                    .method(HttpMethod.POST)
                    .path("/grpc.health.v1.Health/Check")
                    .contentType(MediaType.JSON_UTF_8)
                    .build());

            final AggregatedHttpResponse httpResponse = client.execute(request).aggregate().join();

            assertThat(httpResponse.status(), equalTo(HttpStatus.UNAUTHORIZED));
        }

        @Test
        void request_with_invalid_token_responds_Unauthorized() {
            WebClient client = WebClient.builder(server.httpUri())
                    .addHeader(HEADER_NAME, "invalid-token")
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
        void request_with_valid_token_responds_OK() {
            WebClient client = WebClient.builder(server.httpUri())
                    .addHeader(HEADER_NAME, TOKEN)
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

