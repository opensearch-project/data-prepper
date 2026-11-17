/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TokenUriValidatorTest {
    private final TokenUriValidator objectUnderTest = new TokenUriValidator();

    @ParameterizedTest
    @ValueSource(strings = {"https://oauth2.googleapis.com/token", "https://OAUTH2.googleapis.com/token",
            "https://oauth2.googleapis.com:443/token"})
    void validate_accepts_google_token_endpoint(final String tokenUri) {
        assertThat(objectUnderTest.validate(tokenUri), equalTo(URI.create(tokenUri)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://oauth2.googleapis.com/token", "https:///token", "file:/etc/passwd"})
    void validate_rejects_non_https_or_hostless(final String tokenUri) {
        final IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> objectUnderTest.validate(tokenUri));

        assertThat(exception.getMessage(), containsString("token_uri must be an https Google endpoint"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://evil.example.com/token", "https://www.googleapis.com/token",
            "https://oauth2.googleapis.com.evil.example.com/token"})
    void validate_rejects_other_hosts(final String tokenUri) {
        final IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> objectUnderTest.validate(tokenUri));

        assertThat(exception.getMessage(), containsString("token_uri host must be oauth2.googleapis.com"));
    }

    @Test
    void validate_rejects_non_default_port() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.validate("https://oauth2.googleapis.com:8443/token"));

        assertThat(exception.getMessage(), containsString("default https port"));
    }

    @Test
    void validate_rejects_malformed_uri() {
        final IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> objectUnderTest.validate("https://bad host/token"));

        assertThat(exception.getMessage(), containsString("Invalid token_uri"));
    }
}
