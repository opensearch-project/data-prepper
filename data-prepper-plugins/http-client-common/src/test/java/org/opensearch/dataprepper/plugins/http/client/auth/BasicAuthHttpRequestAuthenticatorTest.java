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

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BasicAuthHttpRequestAuthenticatorTest {

    @Test
    void applyAuth_addsBasicAuthorizationHeader() {
        final BasicAuthHttpRequestAuthenticator authenticator =
                new BasicAuthHttpRequestAuthenticator("admin", "secret");

        final HttpHeadersBuilder builder = HttpHeaders.builder();
        authenticator.applyAuth(builder);

        final String expectedCredentials = Base64.getEncoder()
                .encodeToString("admin:secret".getBytes(StandardCharsets.UTF_8));
        assertThat(builder.build().get(HttpHeaderNames.AUTHORIZATION),
                equalTo("Basic " + expectedCredentials));
    }

    @Test
    void applyAuth_encodesSpecialCharacters() {
        final BasicAuthHttpRequestAuthenticator authenticator =
                new BasicAuthHttpRequestAuthenticator("user@domain", "p@ss:word!");

        final HttpHeadersBuilder builder = HttpHeaders.builder();
        authenticator.applyAuth(builder);

        final String header = builder.build().get(HttpHeaderNames.AUTHORIZATION);
        assertThat(header, notNullValue());
        assertThat(header, containsString("Basic "));

        final String encoded = header.substring("Basic ".length());
        final String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        assertThat(decoded, equalTo("user@domain:p@ss:word!"));
    }

    @Test
    void applyAuth_handlesEmptyPassword() {
        final BasicAuthHttpRequestAuthenticator authenticator =
                new BasicAuthHttpRequestAuthenticator("user", "");

        final HttpHeadersBuilder builder = HttpHeaders.builder();
        authenticator.applyAuth(builder);

        final String encoded = builder.build().get(HttpHeaderNames.AUTHORIZATION).substring("Basic ".length());
        final String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        assertThat(decoded, equalTo("user:"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void constructor_throwsOnNullOrBlankUsername(final String username) {
        assertThrows(IllegalArgumentException.class,
                () -> new BasicAuthHttpRequestAuthenticator(username, "password"));
    }

    @Test
    void constructor_throwsOnNullPassword() {
        assertThrows(IllegalArgumentException.class,
                () -> new BasicAuthHttpRequestAuthenticator("user", null));
    }
}
