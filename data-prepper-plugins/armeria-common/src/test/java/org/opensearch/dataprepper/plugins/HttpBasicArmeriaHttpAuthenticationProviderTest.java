/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.auth.BasicToken;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

class HttpBasicArmeriaHttpAuthenticationProviderTest {

    private static final String USERNAME = UUID.randomUUID().toString();
    private static final String PASSWORD = UUID.randomUUID().toString();

    @RegisterExtension
    static ServerExtension server = new ServerExtension() {
        @Override
        protected void configure(final ServerBuilder sb) {
            sb.service("/test", (ctx, req) -> HttpResponse.of(200));
            sb.service("/health", (ctx, req) -> HttpResponse.of(200));

            final HttpBasicAuthenticationConfig config = mock(HttpBasicAuthenticationConfig.class);
            when(config.getUsername()).thenReturn(USERNAME);
            when(config.getPassword()).thenReturn(PASSWORD);
            Optional<Function<? super HttpService, ? extends HttpService>> optionalAuthDecorator =
                    new HttpBasicArmeriaHttpAuthenticationProvider(config).getAuthenticationDecorator();
            optionalAuthDecorator.ifPresent(authDecorator -> sb.decorator("regex:^/(?!health$).*$", authDecorator));
        }
    };

    @Nested
    class ConstructorTests {
        private HttpBasicAuthenticationConfig config;

        @BeforeEach
        void setUp() {
            config = mock(HttpBasicAuthenticationConfig.class);

        }

        private HttpBasicArmeriaHttpAuthenticationProvider createObjectUnderTest() {
            return new HttpBasicArmeriaHttpAuthenticationProvider(config);
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

            final AggregatedHttpResponse httpResponse = client.get("/test").aggregate().join();

            assertThat(httpResponse.status(), equalTo(HttpStatus.UNAUTHORIZED));
        }

        @Test
        void httpRequest_with_incorrect_authentication_responds_Unauthorized() {
            final WebClient client = WebClient.builder(server.httpUri())
                    .auth(BasicToken.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                    .build();

            final AggregatedHttpResponse httpResponse = client.get("/test").aggregate().join();

            assertThat(httpResponse.status(), equalTo(HttpStatus.UNAUTHORIZED));
        }

        @Test
        void httpRequest_with_correct_authentication_responds_OK() {
            final WebClient client = WebClient.builder(server.httpUri())
                    .auth(BasicToken.of(USERNAME, PASSWORD))
                    .build();

            final AggregatedHttpResponse httpResponse = client.get("/test").aggregate().join();

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));
        }

        @Test
        void httpRequest_without_authentication_responds_OK_for_health() {
            final WebClient client = WebClient.builder(server.httpUri())
                    .build();

            final AggregatedHttpResponse httpResponse = client.get("/health").aggregate().join();

            assertThat(httpResponse.status(), equalTo(HttpStatus.OK));
        }
    }
}