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

import com.linecorp.armeria.common.HttpHeadersBuilder;

public interface ScrapeRequestAuthenticator {

    void applyAuth(HttpHeadersBuilder builder);

    static ScrapeRequestAuthenticator create(final ScrapeAuthenticationConfig config) {
        if (config == null) {
            return null;
        }
        if (config.getHttpBasic() != null) {
            return new BasicAuthenticator(
                    config.getHttpBasic().getUsername(),
                    config.getHttpBasic().getPassword());
        }
        if (config.getBearerToken() != null) {
            return new BearerTokenAuthenticator(config.getBearerToken());
        }
        return null;
    }
}