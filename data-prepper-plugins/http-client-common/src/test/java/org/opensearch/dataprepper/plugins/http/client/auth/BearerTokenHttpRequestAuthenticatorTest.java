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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BearerTokenHttpRequestAuthenticatorTest {

    @Test
    void applyAuth_addsBearerAuthorizationHeader() {
        final BearerTokenHttpRequestAuthenticator authenticator =
                new BearerTokenHttpRequestAuthenticator("my-jwt-token");

        final HttpHeadersBuilder builder = HttpHeaders.builder();
        authenticator.applyAuth(builder);

        assertThat(builder.build().get(HttpHeaderNames.AUTHORIZATION),
                equalTo("Bearer my-jwt-token"));
    }

    @Test
    void applyAuth_handlesLongToken() {
        final String longToken = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature";
        final BearerTokenHttpRequestAuthenticator authenticator =
                new BearerTokenHttpRequestAuthenticator(longToken);

        final HttpHeadersBuilder builder = HttpHeaders.builder();
        authenticator.applyAuth(builder);

        assertThat(builder.build().get(HttpHeaderNames.AUTHORIZATION),
                equalTo("Bearer " + longToken));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void constructor_throwsOnNullOrBlankToken(final String token) {
        assertThrows(IllegalArgumentException.class,
                () -> new BearerTokenHttpRequestAuthenticator(token));
    }
}
