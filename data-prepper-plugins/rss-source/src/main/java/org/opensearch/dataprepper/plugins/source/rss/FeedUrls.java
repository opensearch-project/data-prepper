/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

final class FeedUrls {

    private FeedUrls() {
    }

    /**
     * Strips the query string from a URL so tokens carried as query parameters
     * are not exposed in logs or index names.
     */
    static String redact(final String url) {
        if (url == null) {
            return null;
        }
        final int queryStart = url.indexOf('?');
        return queryStart < 0 ? url : url.substring(0, queryStart) + "?<redacted>";
    }
}
