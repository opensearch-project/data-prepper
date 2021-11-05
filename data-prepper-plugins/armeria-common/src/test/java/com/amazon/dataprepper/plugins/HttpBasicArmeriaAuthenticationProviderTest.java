package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.auth.BasicToken;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

class HttpBasicArmeriaAuthenticationProviderTest {

    private static final String USERNAME = UUID.randomUUID().toString();
    private static final String PASSWORD = UUID.randomUUID().toString();

    @RegisterExtension
    static ServerExtension server = new ServerExtension() {
        @Override
        protected void configure(final ServerBuilder sb) {
            sb.service("/test", (ctx, req) -> HttpResponse.of(200));

            final HttpBasicAuthenticationConfig config = mock(HttpBasicAuthenticationConfig.class);
            when(config.getUsername()).thenReturn(USERNAME);
            when(config.getPassword()).thenReturn(PASSWORD);
            new HttpBasicArmeriaAuthenticationProvider(config).addAuthenticationDecorator(sb);
        }
    };

    @Nested
    class ConstructorTests {
        private HttpBasicAuthenticationConfig config;

        @BeforeEach
        void setUp() {
            config = mock(HttpBasicAuthenticationConfig.class);

        }

        private HttpBasicArmeriaAuthenticationProvider createObjectUnderTest() {
            return new HttpBasicArmeriaAuthenticationProvider(config);
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
    }
}