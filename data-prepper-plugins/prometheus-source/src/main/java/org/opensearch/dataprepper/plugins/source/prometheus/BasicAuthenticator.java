/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.prometheus;

import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpHeadersBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class BasicAuthenticator implements ScrapeRequestAuthenticator {

    private final String encodedCredentials;

    BasicAuthenticator(final String username, final String password) {
        final String credentials = username + ":" + password;
        this.encodedCredentials = Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void applyAuth(final HttpHeadersBuilder builder) {
        builder.add(HttpHeaderNames.AUTHORIZATION, "Basic " + encodedCredentials);
    }
}