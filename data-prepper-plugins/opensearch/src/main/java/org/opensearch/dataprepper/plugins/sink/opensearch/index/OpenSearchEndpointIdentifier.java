/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.net.URI;
import java.util.List;

public class OpenSearchEndpointIdentifier {

    private OpenSearchEndpointIdentifier() {
    }

    public static String extractCollectionId(final List<String> hosts) {
        final String hostname = getHostname(hosts);
        return hostname.split("\\.")[0];
    }

    public static String extractDomainName(final List<String> hosts) {
        final String hostname = getHostname(hosts);
        final String prefix = hostname.split("\\.")[0];
        final String withoutSearchPrefix = prefix.replaceFirst("^(search-|vpc-)", "");
        final int lastHyphen = withoutSearchPrefix.lastIndexOf('-');
        if (lastHyphen <= 0) {
            throw new IllegalArgumentException(
                    "Unable to extract domain name from host: " + hostname +
                            ". Please set the 'domain_name' option in semantic_enrichment config.");
        }
        return withoutSearchPrefix.substring(0, lastHyphen);
    }

    static String getHostname(final List<String> hosts) {
        if (hosts == null || hosts.isEmpty()) {
            throw new IllegalArgumentException("Hosts list is empty, cannot extract endpoint identifier");
        }
        final String hostname = URI.create(hosts.get(0)).getHost();
        if (hostname == null) {
            throw new IllegalArgumentException("Unable to parse hostname from: " + hosts.get(0));
        }
        return hostname;
    }
}
