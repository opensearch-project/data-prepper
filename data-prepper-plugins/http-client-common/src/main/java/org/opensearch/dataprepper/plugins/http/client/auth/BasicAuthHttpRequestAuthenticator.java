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
import com.linecorp.armeria.common.HttpHeadersBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class BasicAuthHttpRequestAuthenticator implements HttpRequestAuthenticator {

    private final String headerValue;

    public BasicAuthHttpRequestAuthenticator(final String username, final String password) {
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("Username must not be null or blank");
        }
        if (password == null) {
            throw new IllegalArgumentException("Password must not be null");
        }
        final String credentials = username + ":" + password;
        this.headerValue = "Basic " + Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void applyAuth(final HttpHeadersBuilder builder) {
        builder.add(HttpHeaderNames.AUTHORIZATION, headerValue);
    }
}
