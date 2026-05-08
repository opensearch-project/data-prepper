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

final class BearerTokenAuthenticator implements ScrapeRequestAuthenticator {

    private final String token;

    BearerTokenAuthenticator(final String token) {
        this.token = token;
    }

    @Override
    public void applyAuth(final HttpHeadersBuilder builder) {
        builder.add(HttpHeaderNames.AUTHORIZATION, "Bearer " + token);
    }
}