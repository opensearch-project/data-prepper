/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Decides which peer host names may name a synthesized dependency node. Used only while a
 * {@link SpanStateData} is being built; it is not persisted with the span.
 */
public class DependencyNamingPolicy {
    /**
     * Host names derived from an IP address, one per instance or pod: EC2 private
     * ({@code ip-10-0-0-5.ec2.internal}), EC2 public ({@code ec2-1-2-3-4.compute-1.amazonaws.com})
     * and Kubernetes pod DNS ({@code 10-0-0-5.ns.pod.cluster.local}).
     */
    public static final List<String> DEFAULT_HOSTNAME_DENYLIST_PATTERNS = List.of(
            "^ip-\\d{1,3}-\\d{1,3}-\\d{1,3}-\\d{1,3}(\\..*)?$",
            "^ec2-\\d{1,3}-\\d{1,3}-\\d{1,3}-\\d{1,3}\\..*$",
            "^\\d{1,3}-\\d{1,3}-\\d{1,3}-\\d{1,3}\\..*$");

    public static final DependencyNamingPolicy DEFAULT = new DependencyNamingPolicy(DEFAULT_HOSTNAME_DENYLIST_PATTERNS);

    private final List<Pattern> hostnameDenylist;

    public DependencyNamingPolicy(final List<String> hostnameDenylistPatterns) {
        this.hostnameDenylist = hostnameDenylistPatterns == null
                ? Collections.emptyList()
                : hostnameDenylistPatterns.stream()
                        .map(pattern -> Pattern.compile(pattern, Pattern.CASE_INSENSITIVE))
                        .collect(Collectors.toList());
    }

    /**
     * @param host A peer host name without port
     * @return true if the host matches a configured denylist pattern
     */
    public boolean isDeniedHost(final String host) {
        if (host == null) {
            return false;
        }
        for (final Pattern pattern : hostnameDenylist) {
            if (pattern.matcher(host).matches()) {
                return true;
            }
        }
        return false;
    }
}
