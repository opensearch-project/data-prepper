/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.http.client.auth;

import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpHeaders;
import com.linecorp.armeria.common.HttpHeadersBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HttpRequestAuthenticatorTest {

    @Mock
    private HttpClientAuthenticationConfig config;

    @Mock
    private HttpClientAuthenticationConfig.HttpBasicConfig httpBasicConfig;

    @Test
    void create_returnsNullWhenConfigIsNull() {
        assertThat(HttpRequestAuthenticator.create(null), is(nullValue()));
    }

    @Test
    void create_returnsNullWhenNoAuthConfigured() {
        when(config.getHttpBasic()).thenReturn(null);
        when(config.getBearerToken()).thenReturn(null);

        assertThat(HttpRequestAuthenticator.create(config), is(nullValue()));
    }

    @Test
    void create_returnsBasicAuthenticatorWhenHttpBasicConfigured() {
        when(httpBasicConfig.getUsername()).thenReturn("user");
        when(httpBasicConfig.getPassword()).thenReturn("pass");
        when(config.getHttpBasic()).thenReturn(httpBasicConfig);

        final HttpRequestAuthenticator authenticator = HttpRequestAuthenticator.create(config);

        assertThat(authenticator, instanceOf(BasicAuthHttpRequestAuthenticator.class));

        final HttpHeadersBuilder builder = HttpHeaders.builder();
        authenticator.applyAuth(builder);
        assertThat(builder.build().get(HttpHeaderNames.AUTHORIZATION), containsString("Basic "));
    }

    @Test
    void create_returnsBearerTokenAuthenticatorWhenBearerTokenConfigured() {
        when(config.getHttpBasic()).thenReturn(null);
        when(config.getBearerToken()).thenReturn("my-token");

        final HttpRequestAuthenticator authenticator = HttpRequestAuthenticator.create(config);

        assertThat(authenticator, instanceOf(BearerTokenHttpRequestAuthenticator.class));

        final HttpHeadersBuilder builder = HttpHeaders.builder();
        authenticator.applyAuth(builder);
        assertThat(builder.build().get(HttpHeaderNames.AUTHORIZATION), containsString("Bearer my-token"));
    }

    @Test
    void create_prefersBasicAuthOverBearerToken() {
        when(httpBasicConfig.getUsername()).thenReturn("user");
        when(httpBasicConfig.getPassword()).thenReturn("pass");
        when(config.getHttpBasic()).thenReturn(httpBasicConfig);

        final HttpRequestAuthenticator authenticator = HttpRequestAuthenticator.create(config);

        assertThat(authenticator, instanceOf(BasicAuthHttpRequestAuthenticator.class));
    }
}
