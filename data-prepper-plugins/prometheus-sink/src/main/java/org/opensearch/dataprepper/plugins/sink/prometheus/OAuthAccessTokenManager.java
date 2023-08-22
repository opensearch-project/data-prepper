/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus;

import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.BearerTokenOptions;

public class OAuthAccessTokenManager {

    public String getAccessToken(final BearerTokenOptions bearerTokenOptions) {
        //TODO implementation
        return null;
    }

    public boolean isTokenExpired(final String token){
        //TODO implementation
        return false;
    }
}
