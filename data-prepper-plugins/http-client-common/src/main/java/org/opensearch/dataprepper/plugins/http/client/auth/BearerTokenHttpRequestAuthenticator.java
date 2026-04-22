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

public class BearerTokenHttpRequestAuthenticator implements HttpRequestAuthenticator {

    private final String headerValue;

    public BearerTokenHttpRequestAuthenticator(final String token) {
        this.headerValue = "Bearer " + token;
    }

    @Override
    public void applyAuth(final HttpHeadersBuilder builder) {
        builder.add(HttpHeaderNames.AUTHORIZATION, headerValue);
    }
}
