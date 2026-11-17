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

import java.net.URI;
import java.util.Locale;

/**
 * Restricts the service-account token endpoint to Google's OAuth2 token host. Ported from the
 * ml-commons google_cloud connector (opensearch-project/ml-commons#4921).
 */
class TokenUriValidator {
    static final String OAUTH2_TOKEN_HOST = "oauth2.googleapis.com";

    URI validate(final String tokenUri) {
        final URI uri;
        try {
            uri = URI.create(tokenUri);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid token_uri: " + tokenUri);
        }
        final String scheme = uri.getScheme();
        final String host = uri.getHost();
        if (!"https".equalsIgnoreCase(scheme) || host == null) {
            throw new IllegalArgumentException("token_uri must be an https Google endpoint, got: " + tokenUri);
        }
        if (uri.getRawUserInfo() != null) {
            throw new IllegalArgumentException("token_uri must not contain user info");
        }
        if (!OAUTH2_TOKEN_HOST.equals(host.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("token_uri host must be " + OAUTH2_TOKEN_HOST + ", got: " + host);
        }
        final int port = uri.getPort();
        if (port != -1 && port != 443) {
            throw new IllegalArgumentException("token_uri must use the default https port, got port: " + port);
        }
        return uri;
    }
}
