/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.common.AggregatedHttpRequest;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HttpHeaderExtractor {

    static final Set<String> SENSITIVE_HEADERS = Set.of(
            "authorization",
            "proxy-authorization",
            "cookie",
            "set-cookie",
            "www-authenticate",
            "proxy-authenticate",
            "x-api-key",
            "x-csrf-token",
            "x-xsrf-token",
            "x-auth-token",
            "x-amz-security-token",
            "x-amz-credential"
    );

    private final Collection<String> metadataHeaders;

    public HttpHeaderExtractor(@Nonnull final Collection<String> metadataHeaders) {
        this.metadataHeaders = metadataHeaders;
    }

    public Map<String, Object> extractHeaders(final AggregatedHttpRequest aggregatedHttpRequest) {
        if (metadataHeaders.isEmpty()) {
            return Collections.emptyMap();
        }

        final Set<String> headerNames = metadataHeaders.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        final Map<String, Object> headers = new HashMap<>();
        for (String headerName : headerNames) {
            if (isSensitiveHeader(headerName)) {
                continue;
            }
            List<String> values = aggregatedHttpRequest.headers().getAll(headerName);
            if (!values.isEmpty()) {
                headers.put(headerName, values.size() == 1 ? values.get(0) : Collections.unmodifiableList(values));
            }
        }

        return headers;
    }

    static boolean isSensitiveHeader(final String headerName) {
        return SENSITIVE_HEADERS.contains(headerName.toLowerCase());
    }
}
