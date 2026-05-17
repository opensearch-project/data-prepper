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

import com.linecorp.armeria.common.HttpHeadersBuilder;

/**
 * Interface for applying authentication to outgoing HTTP requests.
 */
public interface HttpRequestAuthenticator {

    void applyAuth(HttpHeadersBuilder builder);

    static HttpRequestAuthenticator create(final HttpClientAuthenticationConfig config) {
        if (config == null) {
            return null;
        }
        if (config.getHttpBasic() != null) {
            return new BasicAuthHttpRequestAuthenticator(
                    config.getHttpBasic().getUsername(),
                    config.getHttpBasic().getPassword());
        }
        if (config.getBearerToken() != null) {
            return new BearerTokenHttpRequestAuthenticator(config.getBearerToken());
        }
        return null;
    }
}
